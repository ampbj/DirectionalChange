using DataFrames
using CSV
using Dates
using MarketRegime
using PyCall
using MarketData
using Pipe: @pipe
python_lib_path = normpath(abspath(@__FILE__), "../../../../../Python/market_regime_plot")
pushfirst!(PyVector(pyimport("sys")."path"), python_lib_path)
MRP = pyimport("market_regime_plot")
pd = pyimport("pandas")
np = pyimport("numpy")


data = normpath(abspath(@__FILE__), "../../../../../data/fx_usd_jpy/fx_usd_jpy_close_only/all.csv")
result_folder = normpath(abspath(@__FILE__), "../../../result/")

function test_dr_forex(df::DataFrame, dc_offset::AbstractVector, result_path::String ;save_file=false, plt=false, plt_data_freq="d")
	df[!,:Timestamp] = parse.(DateTime, df.Timestamp, dateformat"yyyymmdd\ HHMMSS")
	data = @pipe MarketRegime.init(df, dc_offset) |> MarketRegime.prepare(_...) |> MarketRegime.fit(_...)
	if save_file
		writing_csv = @task CSV.write(result_path, data)
		schedule(writing_csv)
	end
	if plt
		if save_file
			wait(writing_csv)
		end
		plot_graph(result_path, plt_data_freq)
	end
	return nothing
end

function plot_graph(result_path::String, plt_data_freq::String)
	df = pd.read_csv(result_path)
	df.set_index("Timestamp", inplace=true)
	df.index = pd.to_datetime(df.index)
	MRP_main = MRP.Main(df, data_freq=plt_data_freq)
	MRP_main.plot_market_regime()
end

function market_data_test()
	t = Dates.now()
	data = DataFrame(yahoo(:SPY, YahooOpt(period1=t - Year(1), period2=t, interval="1d")))
	data = convert(DataFrame, data)[!, [:timestamp, :Close ]]
	result_path = normpath(abspath(@__FILE__), "../../../result/spy_one_year.csv")
	test_dr_forex(data, [0.1], result_path, save_file=true, plt=false)
end
df = CSV.read(data, DataFrame)
result_path = result_folder * "usd_jpy_0_01.csv"
test_dr_forex(df,[0.01], result_path, save_file=true, plt=true, plt_data_freq="m")
# market_data_test()