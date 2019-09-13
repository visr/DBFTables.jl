module DBFTables

using DataFrames, Printf, Tables, WeakRefStrings

# Read DBF files in xBase format
# Files written in this format have the extension .dbf
# Implemented: dBase III+ (w/o memo)

# changes to note:
# String: strip to rstrip
# FieldDescriptor.nam: String to Symbol
# TODO implement iterator and getindex based on .len
# TODO implement Tables.columns
# TODO rename read_dbf and remove filename version?
# Date DataType as Date
# removed DBF from struct names

struct FieldDescriptor
	nam::Symbol
	typ::DataType
	len::Int8
	dec::Int8
end

struct Header
	version::UInt8
	lastUpdate::String
	records::Int32
	hsize::Int16
	rsize::Int16
	incomplete::Bool
	encrypted::Bool
	mdx::Bool
	langId::UInt8
	fields::Vector{FieldDescriptor}
end

function typemap(fld::Char, dec::UInt8)
	rt = Nothing
	if fld == 'C'
		rt = String
	elseif fld == 'D'
		rt = String
	elseif fld == 'N'
		if dec > 0
			rt = Float64
		else
			# TODO do we want this?
			rt = Int
		end
	elseif fld == 'F' || fld == 'O'
		rt = Float64
	elseif fld == 'I' || fld == '+'
		rt = Int
	elseif fld == 'L'
		rt = Bool
	else
		throw(ArgumentError("Unknown record type $fld"))
	end
	return rt
end

function read_dbf_field(io::IO)
	field_name_raw = String(read!(io, Vector{UInt8}(undef, 11)))
	field_name = Symbol(strip(replace(field_name_raw, '\0'=>' ')))
	field_type = read(io, Char)
	read(io, Int32) # skip
	field_len = read(io, UInt8)
	field_dec = read(io, UInt8)
	read(io, 14) # reserved
	jltype = typemap(field_type, field_dec)
	return FieldDescriptor(field_name, jltype, field_len, field_dec)
end

function Header(io::IO)
	ver = read(io, UInt8)
	date = read!(io, Vector{UInt8}(undef, 3)) # 0x01
	last_update = @sprintf("%4d%02d%02d", date[1]+1900, date[2], date[3])
	records = read(io, Int32) # 0x04
	hsize = read(io, Int16) # 0x08
	rsize = read(io, Int16) # 0x0A
	read(io, Int16) # reserved # 0x0C
	incomplete = Bool(read(io, UInt8)) # 0x0E
	encrypted = Bool(read(io, UInt8)) # 0x0F
	read!(io, Vector{UInt8}(undef, 12)) # reserved
	mdx = Bool(read(io, UInt8)) # 0x1C
	langId = read(io, UInt8) # 0x1D
	read!(io, Vector{UInt8}(undef, 2)) # reserved # 0x1E
	fields = FieldDescriptor[]

	while !eof(io)
		push!(fields, read_dbf_field(io))
		p = position(io)
		trm = read(io, UInt8)
		if trm == 0xD
			break
		else
			seek(io, p)
		end
	end

	return Header(ver, last_update, records, hsize, rsize,
					 incomplete, encrypted, mdx, langId,
					 fields)
end

miss(x) = ifelse(x === nothing, missing, x)

function dbf_value(T::Type{Bool}, str::AbstractString)
	char = first(str)
	if char in "YyTt"
		true
	elseif char in "NnFf"
		false
	elseif char == '?'
		missing
	else
		throw(ArgumentError("Unknown logical $char"))
	end
end

dbf_value(T::Union{Type{Int}, Type{Float64}}, str::AbstractString) = miss(tryparse(T, str))
dbf_value(T::Type{String}, str::AbstractString) = rstrip(str)
dbf_value(T::Type{Nothing}, str::AbstractString) = missing

function read_dbf_records!(io::IO, df::DataFrame, header::Header)
	# create Tables.Schema
	names = Tuple(getfield.(header.fields, :nam))
	# since missing is always supported, add it to the schema types
	types_notmissing = Tuple(getfield.(header.fields, :typ))
	types = Tuple{map(T -> Union{T, Missing}, types_notmissing)...}
	dbfschema = Tables.Schema(names, types)
	nbytes = Tuple(getfield.(header.fields, :len))
	@show names types dbfschema
	nrow = header.records
	ncol = length(header.fields)

	for _ in 1:nrow
		# skip deleted records
		read(io, UInt8) == 0x2A && continue
		r = NamedTuple{names, types}(
			(dbf_value(types_notmissing[col], String(read(io, nbytes[col]))) for col in 1:ncol)
		)
		@show r
		push!(df, r)
	end
	return df
end

# FieldDescriptor, use type parameters?
# should the schema go in here? don't think so
struct Table
	header::Header
	data::Vector{UInt8}  # WeakRefString reference this
	strings::StringArray{WeakRefString{UInt8}, 2}
end

# struct Row{names, T} where {names, T <: Tuple}
# 	nt::NamedTuple{names, types}
# end

# load whole table in a mmapped Vector{UInt8}?
# or can we mmap a Vector{NamedTuple?}
function Table(io::IO)
	header = Header(io)
	@show position(io)
	df = DataFrame(map(f->Union{f.typ,Missing}, header.fields), getfield.(header.fields, :nam), 0)
	read_dbf_records!(io, df, header)
	# TODO how to account for deleted records?
	# -> reserve the memory, just make sure to skip them when needed
	# -> we should probably add a test file with skipped records
	seek(io, header.hsize)  # go back to the start of the data
	data = Vector{UInt8}(undef, header.rsize * header.records)
	read!(io, data)
	strings = _create_stringarray(header, data)
	# 6 fields 7 records
	# 225 bytes header, 55 byte record
	# 55 includes the deleted marker
	# 7 * 55 = 385 bytes data += 225 = 610 bytes header+data = 610
	# plus there is a 1A sub byte at the end, seen more often but not always, sometime multiple
	# http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm
	t = Table(header, data, strings)
	return df, t
end

function _create_stringarray(header::Header, data::AbstractVector)
	# first make the lengths and offsets for a single record
	lengths_record = UInt32.(getfield.(header.fields, :len))
	offsets_record = vcat(0, cumsum(lengths_record)[1:end-1]) .+ 1

	# the lengths are equal for each record
	lengths = repeat(lengths_record, 1, header.records)
	# the offsets accumulate over records with the record size
	row_offsets = range(0; length=header.records, step=header.rsize)
	offsets = repeat(offsets_record, 1, header.records)
	offsets .+= reshape(row_offsets, 1, :)

	StringArray{WeakRefString{UInt8}, 2}(data, offsets, lengths)
end

end # module
# https://github.com/JuliaData/CSV.jl/issues/482#issuecomment-523742097
