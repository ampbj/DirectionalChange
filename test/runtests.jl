using MarketRegime
using Test
using DataFrames
using MarketData

@testset "Testing MarketRegime.jl" begin
    data = DataFrame(cl)
    DataFrames.rename!(data, [:Timestamp, :Price])
    dc_offset = [0.01,0.02]
    @test names(MarketRegime.init(data, dc_offset)[1]) == ["Timestamp", "Price"]
    @test ncol(MarketRegime.prepare(data, dc_offset)[1]) == 7
    data = MarketRegime.fit(data, dc_offset)
    @test !isempty(data[.!isempty.(data[!,"Event_$(dc_offset[2])"]), :])
    @test !isempty(data[.!isempty.(data[!,"Event_$(dc_offset[1])"]), :])
    @test !isempty(data[.!isempty.(data[!,"pct_change"]), :])
    @test !isempty(data[.!isempty.(data[!,"OSV"]), :])
    @test !isempty(data[(data[!,"BBTheta"]) .!= false, :])
end
