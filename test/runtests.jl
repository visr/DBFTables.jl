using DBFTables
using Test

test_dbf_path = joinpath(@__DIR__, "test.dbf")
df = DBFTables.Table(open(test_dbf_path))

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

@test size(df,1) == 7 # records
@test size(df,2) == 6 # fields
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

float_field_descriptor = DBFTables.FieldDescriptor(:FloatField, Float64, 8, 1)
bool_field_descriptor = DBFTables.FieldDescriptor(:BoolField, Bool, 1, 0)
fields = [float_field_descriptor, bool_field_descriptor]
fieldnames = Tuple(Symbol.(getfield.(fields, :nam)))
fieldtypes = Tuple{getfield.(fields, :typ)...}
nt = NamedTuple{fieldnames, fieldtypes}((2.1, true))
