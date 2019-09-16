# DBFTables

[![Build Status](https://travis-ci.org/JuliaData/DBFTables.jl.svg?branch=master)](https://travis-ci.org/JuliaData/DBFTables.jl)
[![Coverage Status](https://coveralls.io/repos/JuliaData/DBFTables.jl/badge.svg?branch=master&service=github)](https://coveralls.io/github/JuliaData/DBFTables.jl?branch=master)
[![codecov.io](http://codecov.io/github/JuliaData/DBFTables.jl/coverage.svg?branch=master)](http://codecov.io/github/JuliaData/DBFTables.jl?branch=master)

Read xBase / dBASE III+ [.dbf](https://en.wikipedia.org/wiki/.dbf) files in Julia.

## Usage

```julia
using DBFTables
io = open("test.dbf")
dbf = DBFTables.Table(io)

# whole columns can be retrieved by their name
dbf.INTEGER  # => Union{Missing, Int64}[100, 101, 102, 0, 2222222222, 4444444444, missing]

# example function that iterates over the rows and uses two columns
function sumif(dbf)
    total = 0.0
    for row in dbf
        if row.BOOLEAN && !ismissing(row.NUMERIC)
            value += row.NUMERIC
        end
    end
    return total
end

# for other functionality, convert to other Tables
using DataFrames
df = DataFrame(dbf)
```


## Read DBF files in xBase format
Files written in this format have the extension .dbf
Implemented: dBase III+ (w/o memo)

## Resources
- http://shapelib.maptools.org/
- https://www.clicketyclick.dk/databases/xbase/format/
- https://en.wikipedia.org/wiki/.dbf
- http://www.independent-software.com/dbase-dbf-dbt-file-format.html
- https://www.clicketyclick.dk/databases/xbase/format/dbf.html

## Changes
- String: strip to rstrip
- FieldDescriptor.nam: String to Symbol
- change signed fields to unsigned, like FieldDescriptor.len
- TODO implement iterator and getindex based on .len
- TODO implement Tables.columns
- TODO rename read_dbf and remove filename version?
- Date DataType as Date
- removed DBF from struct names
