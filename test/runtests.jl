using DBFTables
using Test
using Tables
using WeakRefStrings
using DataFrames

test_dbf_path = joinpath(@__DIR__, "test.dbf")
dbf = DBFTables.Table(open(test_dbf_path))
df = DataFrame(dbf)

@testset "DataFrame indexing" begin
    @test size(df, 1) == 7 # records
    @test size(df, 2) == 6 # fields
    @test df[2, :CHAR] == "John"
    @test df[1, :DATE] == "19900102"
    @test df[3, :BOOL] == false
    @test df[1, :FLOAT] == 10.21
    @test df[2, :NUMERIC] == 12.21
    @test df[3, :INTEGER] == 102
end

@testset "missing entries" begin
    @test ismissing(df[4, :BOOL])
    @test ismissing(df[5, :FLOAT])
    @test ismissing(df[6, :NUMERIC])
    @test ismissing(df[7, :INTEGER])
end

@testset "header" begin
    h = DBFTables.Header(open(test_dbf_path))
    @test h.version == 3
    @test h.lastUpdate == "20140806"
    @test h.records == 7
    @test length(h.fields) == 6
end

row, st = iterate(dbf)
@test st == 2
row








chkrow = NamedTuple{
    (:CHAR, :DATE, :BOOL, :FLOAT, :NUMERIC, :INTEGER),
    Tuple{Union{Missing, String},Union{Missing, String},Union{Missing, Bool},
    Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Int64}}}(
        ("Bob", "19900102", false, 10.21, 11.21, 100))
row === chkrow
row.CHAR
row == (CHAR=2, DATE="19000102", BOOL=false, FLOAT=10.21, NUMERIC=11.21, INTEGER=100)
# test a few other files
ne_path = "c:/tmp/ne/ne_10m_admin_0_boundary_lines_land/ne_10m_admin_0_boundary_lines_land.dbf"

using BenchmarkTools
# 367.300 μs (250 allocations: 489.03 KiB)
@btime DBFTables.Table($ne_path)
t = DBFTables.Table(ne_path)
r, s = iterate(t)
r.scalerank
unique(t.scalerank)
t.scalerank
# 1.319 s (219477 allocations: 12.24 MiB)
@btime sum(row.scalerank for row in $t)
# 69.800 μs (475 allocations: 18.69 KiB)
@btime sum($(t).scalerank)



t = DBFTables.Table(open(ne_path))
getfield(t, :data)
DBFTables.isdeleted(t)

#=
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
