using DirectionalChange
using Test
using DataFrames
using MarketData

@testset "Testing DirectionalChange.jl" begin
    data = DataFrame(cl)
    DataFrames.rename!(data, [:Timestamp, :Price])
    dc_offset = [0.01,0.02]
    @test names(DirectionalChange.init(data, dc_offset)[1]) == ["Timestamp", "Price"]
    @test ncol(DirectionalChange.prepare(data, dc_offset)[1]) == 7
    data = DirectionalChange.fit(data, dc_offset)
    @test !isempty(data[.!isempty.(data[!,"Event_$(dc_offset[2])"]), :])
    @test !isempty(data[.!isempty.(data[!,"Event_$(dc_offset[1])"]), :])
    @test !isempty(data[.!isempty.(data[!,"pct_change"]), :])
    @test !isempty(data[.!isempty.(data[!,"OSV"]), :])
    @test !isempty(data[(data[!,"BBTheta"]) .!= false, :])
end
