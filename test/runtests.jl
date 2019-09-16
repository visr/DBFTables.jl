using DBFTables
using Test
using Tables
using WeakRefStrings
using DataFrames

test_dbf_path = joinpath(@__DIR__, "test.dbf")
dbf = DBFTables.Table(open(test_dbf_path))

#=
│ Row │ CHAR    │ DATE     │ BOOL    │ FLOAT     │ NUMERIC   │ INTEGER    │
│     │ String⍰ │ String⍰  │ Bool⍰   │ Float64⍰  │ Float64⍰  │ Int64⍰     │
├─────┼─────────┼──────────┼─────────┼───────────┼───────────┼────────────┤
│ 1   │ Bob     │ 19900102 │ 0       │ 10.21     │ 11.21     │ 100        │
│ 2   │ John    │ 20010203 │ 1       │ 100.99    │ 12.21     │ 101        │
│ 3   │ Bill    │ 20100304 │ 0       │ 0.0       │ 13.21     │ 102        │
│ 4   │         │ 19700101 │ missing │ 0.0       │ 0.0       │ 0          │
│ 5   │         │ 19700101 │ 1       │ missing   │ 1.11111e9 │ 2222222222 │
│ 6   │         │ 19700101 │ 1       │ 3.33333e9 │ missing   │ 4444444444 │
│ 7   │         │ 19700101 │ 1       │ 5.55556e9 │ 6.66667e9 │ missing    │
=#

@test_broken size(dbf, 1) == 7 # records
@test_broken size(dbf, 2) == 6 # fields
@test_broken dbf[2, :CHAR] == "John"
@test_broken dbf[1, :DATE] == "19900102"
@test_broken dbf[3, :BOOL] == false
@test_broken dbf[1, :FLOAT] == 10.21
@test_broken dbf[2, :NUMERIC] == 12.21
@test_broken dbf[3, :INTEGER] == 102

# Testing missing record handling
@test_broken ismissing(dbf[4, :BOOL])
@test_broken ismissing(dbf[5, :FLOAT])
@test_broken ismissing(dbf[6, :NUMERIC])
@test_broken ismissing(dbf[7, :INTEGER])

float_field_descriptor = DBFTables.FieldDescriptor(:FloatField, Float64, 8, 1)
bool_field_descriptor = DBFTables.FieldDescriptor(:BoolField, Bool, 1, 0)
fields = [float_field_descriptor, bool_field_descriptor]
fieldnames = Tuple(Symbol.(getfield.(fields, :nam)))
fieldtypes = Tuple{getfield.(fields, :typ)...}
nt = NamedTuple{fieldnames, fieldtypes}((2.1, true))

h = DBFTables.Header(open(test_dbf_path))

# test a few other files
#=
ne_path = "c:/tmp/ne/ne_10m_admin_0_boundary_lines_land/ne_10m_admin_0_boundary_lines_land.dbf"
using BenchmarkTools
# 367.300 μs (250 allocations: 489.03 KiB)
@btime open($ne_path) do io
   DBFTables.Table(io)
end
t = DBFTables.Table(open(ne_path))
getfield(t, :data)
DBFTables.isdeleted(t)

org_path = "c:/tmp/ne/shp_deleted/vector.dbf"
h = DBFTables.Header(open(org_path))
Int(h.records)
t = DBFTables.Table(open(org_path))
DBFTables.isdeleted(t)
count(DBFTables.isdeleted(t))
DataFrame(t)

org_path = "c:/tmp/ne/shp_deleted/vector_edited.dbf"
h = DBFTables.Header(open(org_path))
Int(h.records)
t = DBFTables.Table(open(org_path))
DBFTables.isdeleted(t)
count(DBFTables.isdeleted(t))
DataFrame(t)
=#
