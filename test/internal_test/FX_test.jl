using DataFrames
using CSV
using Dates
using MarketRegime
using PyCall
using Pipe: @pipe


python_lib_path = normpath(abspath(@__FILE__), "../../../../../Python")
pushfirst!(PyVector(pyimport("sys")."path"), python_lib_path)
MRP = pyimport("Market_regime_plot")
pd = pyimport("pandas")

function test_dr_forex(;save_file=false, plt=false, dc_offset=[0.01,0.02])
	data_file_path = normpath(abspath(@__FILE__), "../../../../../data/fx_usd_jpy/fx_usd_jpy_close_only/all.csv")
	df = CSV.read(data_file_path, DataFrame)
	df[!,:Timestamp] = parse.(DateTime, df.Timestamp, dateformat"yyyymmdd\ HHMMSS")
	data = @pipe MarketRegime.init(df, dc_offset) |> MarketRegime.prepare(_...) |> MarketRegime.fit(_...)
	if save_file
		data_result_path = normpath(abspath(@__FILE__), "../../../result/all_usd_jpy.csv")
		writing_csv = @task CSV.write(data_result_path, data)
		schedule(writing_csv)
	end
	if plt
		wait(writing_csv)
		df = pd.read_csv(data_result_path)
		df.set_index("Timestamp", inplace=true)
		df.index = pd.to_datetime(df.index)
		MRP_init = MRP.Market_regime_plot(df, data_freq="m")
		MRP_init.plot_market_regime()
	end
	return data
end

test_dr_forex(save_file=true, plt=true)