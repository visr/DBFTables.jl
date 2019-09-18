using DBFTables
using Test
using Tables
using WeakRefStrings
using DataFrames

test_dbf_path = joinpath(@__DIR__, "test.dbf")
dbf = DBFTables.Table(open(test_dbf_path))
df = DataFrame(dbf)

@test size(df, 1) == 7 # records
@test size(df, 2) == 6 # fields
@test df[2, :CHAR] == "John"
@test df[1, :DATE] == "19900102"
@test df[3, :BOOL] == false
@test df[1, :FLOAT] == 10.21
@test df[2, :NUMERIC] == 12.21
@test df[3, :INTEGER] == 102

# Testing missing record handling
@test ismissing(df[4, :BOOL])
@test ismissing(df[5, :FLOAT])
@test ismissing(df[6, :NUMERIC])
@test ismissing(df[7, :INTEGER])

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
