module DBFTables

using Printf, Tables, WeakRefStrings

struct FieldDescriptor
	nam::Symbol
	typ::DataType
	len::UInt8
	dec::UInt8
end

struct Header
	version::UInt8
	lastUpdate::String
	records::UInt32
	hsize::UInt16
	rsize::UInt16
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
	skip(io, 4)  # skip
	field_len = read(io, UInt8)
	field_dec = read(io, UInt8)
	skip(io, 14)  # reserved
	jltype = typemap(field_type, field_dec)
	return FieldDescriptor(field_name, jltype, field_len, field_dec)
end

function Header(io::IO)
	ver = read(io, UInt8)
	date1 = read(io, UInt8)
	date2 = read(io, UInt8)
	date3 = read(io, UInt8)
	last_update = @sprintf("%4d%02d%02d", date1+1900, date2, date3)
	records = read(io, UInt32)
	hsize = read(io, UInt16)
	rsize = read(io, UInt16)
	skip(io, 2)  # reserved
	incomplete = Bool(read(io, UInt8))
	encrypted = Bool(read(io, UInt8))
	skip(io, 12)  # reserved
	mdx = Bool(read(io, UInt8))
	langId = read(io, UInt8)
	skip(io, 2)  # reserved
	fields = FieldDescriptor[]

	while !eof(io)
		push!(fields, read_dbf_field(io))
		mark(io)
		trm = read(io, UInt8)
		if trm == 0xD
			break
		else
			reset(io)
		end
	end

	return Header(ver, last_update, records, hsize, rsize,
				  incomplete, encrypted, mdx, langId, fields)
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
# String to avoid returning SubString{WeakRefString{UInt8}}
dbf_value(T::Type{String}, str::AbstractString) = String(rstrip(str))
dbf_value(T::Type{Nothing}, str::AbstractString) = missing

struct Table
	header::Header
	data::Vector{UInt8}  # WeakRefString reference this
	strings::StringArray{WeakRefString{UInt8}, 2}
end

header(dbf::Table) = getfield(dbf, :header)
fields(dbf::Table) = header(dbf).fields
strings(dbf::Table) = getfield(dbf, :strings)

function Table(io::IO)
	header = Header(io)
	# consider using mmap here for big dbf files
	data = Vector{UInt8}(undef, header.rsize * header.records)
	read!(io, data)
	strings = _create_stringarray(header, data)
	dbf = Table(header, data, strings)
	return dbf
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

function _create_namedtuple(dbf::Table, row::Integer)
    ncol = length(fields(dbf))
	sch = Tables.Schema(dbf)
	record = strings(dbf)[:, row]
    NamedTuple{sch.names, sch.types}(
        (dbf_value(fields(dbf)[col].typ, record[col]) for col in 1:ncol)
    )
end


# Tables interface

Base.isempty(dbf::Table) = header(dbf).records == 0

# We currently ignore the deleted flag, as I haven't come across a file in the
# wild yet that has them. This QGIS issue seems to suggest that many softwares
# don't handle them, and therefore QGIS itself now also always packs files,
# i.e. removes the deleted records entirely https://issues.qgis.org/issues/11007#note-30
# If needed, the isdeleted functions below can be used to find deleted records.

function isdeleted(dbf::Table, row::Integer)
	data = getfield(dbf, :data)
	i = (row - 1) * header(dbf).rsize + 1
	data[i] == 0x2a
end

function isdeleted(dbf::Table)
	data = getfield(dbf, :data)
	rsize = header(dbf).rsize
	nrow = header(dbf).records
	idx = range(1, step=rsize, length=nrow)
	data[idx] .== 0x2a
end

function Base.iterate(dbf::Table)
    isempty(dbf) && return nothing
	data = getfield(dbf, :data)
	data[1, 1] == 0x2a
    nt = DBFTables._create_namedtuple(dbf, 1)
    return nt, 2
end

function Base.iterate(dbf::Table, st)
    st > header(dbf).records && return nothing
    nt = DBFTables._create_namedtuple(dbf, st)
    return nt, st + 1
end

Tables.istable(::Type{Table}) = true
Tables.rowaccess(::Type{Table}) = true
Tables.columnaccess(::Type{Table}) = true
Tables.rows(dbf::Table) = dbf
Tables.columns(dbf::Table) = dbf

function Tables.schema(dbf::Table)
	names = Tuple(field.nam for field in fields(dbf))
	# since missing is always supported, add it to the schema types
	types = Tuple{(Union{field.typ, Missing} for field in fields(dbf))...}
	Tables.Schema(names, types)
end


Base.propertynames(dbf::Table) = getfield.(getfield(dbf, :header).fields, :nam)

function Base.getproperty(dbf::Table, nm::Symbol)
    col = findfirst(x -> x === nm, propertynames(dbf))
	nrow = header(dbf).records
	type = fields(dbf)[col].typ
	str = strings(dbf)
	[dbf_value(type, str[col, i]) for i = 1:nrow]
end

end # module
