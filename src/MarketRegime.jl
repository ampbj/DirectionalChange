module MarketRegime
using Dates
using DataFrames

function init(data::DataFrame, dc_offset::AbstractVector{<:Number})
    column_names = names(data)
    if ncol(data) > 2 && column_names[0] != "Timestamp" && typeof(column_names[1]) == Float64
        error("Data is not aligned with the required structure!
                Module expects only two columns for the DataFrame: Timestamp and Price")
    end
    rename!(data, [:Timestamp, :Price])
    sort!(dc_offset, rev=true)
    return data, dc_offset
end

function pct_change(input::AbstractVector{<:Number}, period::Int=1)
    res = @view(input[(period + 1):end]) ./ @view(input[1:(end - period)]) .- 1
    [fill(missing, period); res]
end

function prepare(data, dc_offset)
    # preparing dataframe for getting fit
    insertcols!(data, :pct_change => pct_change(data.Price))
    dropmissing!(data, :pct_change)
    insertcols!(data, :TMV => NaN)
    insertcols!(data, :T => NaN)
    insertcols!(data, :R => NaN)
    if length(dc_offset) > 1 
        insertcols!(data, (:BBTheta => false))
        insertcols!(data, :OSV => NaN)
    end
    [insertcols!(data, "Event_$(current_offset_value)" => "")
                    for current_offset_value in dc_offset]
    return data, dc_offset
end

function fit(data, dc_offset)
    rows = Tables.namedtupleiterator(data)
    DC_event = repeat(["init"], 2)
    DC_highest_price = repeat([data[1,:Price]], 2)
    DC_lowest_price = repeat([data[1,:Price]], 2)
    DC_highest_price_index = repeat([data[1, :Timestamp]], 2)
    DC_lowest_price_index = repeat([data[1, :Timestamp]], 2)
    last_dc_offset = last(dc_offset)
    
    for (index, offset_value) in enumerate(dc_offset)
        for row in rows
            current_offset_column = "Event_$(offset_value)"
            last_round = offset_value == last_dc_offset && length(dc_offset) > 1
            if DC_event[index] == "downtrend" || DC_event[index] == "init"
                if row.Price >= (DC_lowest_price[index] * (1 + offset_value))
                    DC_event[index] = "uptrend"
                    data[(data.Timestamp .== row.Timestamp), current_offset_column] = ["Up"]
                    check_null_value = data[(data.Timestamp .== DC_lowest_price_index[index]), current_offset_column]
                    isempty(check_null_value[1]) ?
                        data[data.Timestamp .== DC_lowest_price_index[index], current_offset_column] = ["DXP"] :
                        data[data.Timestamp .== DC_lowest_price_index[index], current_offset_column] = ["Down+DXP"]
                    TMV, T, R = calculate_TMV_T_R(data, current_offset_column, DC_lowest_price_index[index], DC_lowest_price[index], offset_value, "UXP")
                    if !isnan(TMV)
                        data[data.Timestamp .== DC_lowest_price_index[index], :TMV] = [TMV]
                    end
                    if !isnan(T)
                        data[data.Timestamp .== DC_lowest_price_index[index], :T] = [T]
                    end
                    if !isnan(R)
                        data[data.Timestamp .== DC_lowest_price_index[index], :R] = [R]
                    end
                    if last_round
                        osv_value = OSV(data, row.Price, DC_lowest_price_index[index], dc_offset[1], "Down")
                        if !isnan(osv_value)
                            data[(data.Timestamp .== row.Timestamp), "OSV"] = [osv_value]
                        end
                        dc_current_lowest_price = data[(data.Timestamp .== DC_lowest_price_index[index]), "Event_$(dc_offset[1])"]
                        dc_current_lowest_price == ["DXP"] || dc_current_lowest_price == ["Down+DXP"] ?
                            data[(data.Timestamp .== row.Timestamp), "BBTheta"] = [true] :
                            data[(data.Timestamp .== row.Timestamp), "BBTheta"] = [false]
                    end
                    DC_highest_price[index] = row.Price
                    DC_highest_price_index[index] = row.Timestamp
                end
                if row.Price <= DC_lowest_price[index]
                    DC_lowest_price[index] = row.Price
                    DC_lowest_price_index[index] = row.Timestamp     
                end
            end
            if DC_event[index] == "uptrend" || DC_event[index] == "init"
                if row.Price <= (DC_highest_price[index] * (1 - offset_value))
                    DC_event[index] = "downtrend"
                    data[(data.Timestamp .== row.Timestamp), current_offset_column] = ["Down"]
                    check_null_value = data[(data.Timestamp .== DC_highest_price_index[index]), current_offset_column]
                    isempty(check_null_value[1]) ?
                        data[(data.Timestamp .== DC_highest_price_index[index]), current_offset_column] = ["UXP"] :
                        data[(data.Timestamp .== DC_highest_price_index[index]), current_offset_column] = ["Up+UXP"]
                    TMV, T, R = calculate_TMV_T_R(data, current_offset_column, DC_highest_price_index[index], DC_highest_price[index], offset_value, "DXP")
                    println("Value: ", TMV, " ", T, " ", R, " Timestamp: ", DC_highest_price_index[index])
                    if !isnan(TMV)
                        data[data.Timestamp .== DC_highest_price_index[index], :TMV] = [TMV]
                    end
                    if !isnan(T)
                        data[data.Timestamp .== DC_highest_price_index[index], :T] = [T]
                    end
                    if !isnan(R)
                        data[data.Timestamp .== DC_highest_price_index[index], :R] = [R]
                    end
                    if last_round
                        osv_value = OSV(data, row.Price, DC_highest_price_index[index], dc_offset[1], "Up")
                        if !isnan(osv_value)
                            data[(data.Timestamp) .== row.Timestamp, "OSV"] = [osv_value]
                        end
                        dc_current_highest_price = data[(data.Timestamp .== DC_highest_price_index[index]), "Event_$(dc_offset[1])"]
                        dc_current_highest_price == ["UXP"] || dc_current_highest_price == ["Up+UXP"] ?
                             data[(data.Timestamp .== row.Timestamp), "BBTheta"] = [true] :
                             data[(data.Timestamp .== row.Timestamp), "BBTheta"] = [false]
                    end
                    DC_lowest_price[index] = row.Price
                    DC_lowest_price_index[index] = row.Timestamp
                end
                if row.Price >= DC_highest_price[index]
                    DC_highest_price[index] = row.Price
                    DC_highest_price_index[index] = row.Timestamp
                end
            end
        end
    end
    return data
end

# Calculating OSV value as an independent variable used for prediction according to the paper
function OSV(data, price, STheta_extreme_index, BTheta, direction)
    BTheta_column = "Event_$BTheta"
    BTheta_rows = data[isless.(data.Timestamp, STheta_extreme_index),:]
    BTheta_rows = BTheta_rows[.!isempty.(BTheta_rows[!,BTheta_column]),:]
    rows = reverse(Tables.rows(BTheta_rows))
    if !isempty(rows)
        return calculate_OSV_value(direction, price, BTheta, rows)
    else 
        return NaN
    end
end

function calculate_OSV_value(direction, price, BTheta, rows)
    BTheta_column = "Event_$BTheta"
    if direction == "Down"
        alternate_direction_value = "Down+DXP"
    end
    if direction == "Up"
        alternate_direction_value = "Up+UXP"
    end
    for row in eachrow(rows)
        if row[1][BTheta_column] == direction || row[1][BTheta_column] == alternate_direction_value
            PDCC_BTheta = row[1].Price
            OSV_value = ((price - PDCC_BTheta) /
                            PDCC_BTheta) / BTheta
            return OSV_value
        end
    end
    return NaN
end
function calculate_TMV_T_R(data, current_offset_column, current_ext_time, current_ext_price, theta, direction)
    if direction == "UXP"
        alternate_direction = "Up+UXP"
    end
    if direction == "DXP"
        alternate_direction = "Down+UXP"
    end
    found_data = data[findall(in([direction, alternate_direction]), data[!,current_offset_column]), :]
    if !isempty(found_data)
        found_data = last(found_data)
        previous_ext_price = found_data.Price
        previous_ext_time = found_data.Timestamp
        TMV = (current_ext_price - previous_ext_price) / (previous_ext_price * theta)
        T = Date(current_ext_time) - Date(previous_ext_time)
        T = T.value
        if T == 0
            T = 1
        end
        R = abs((TMV / T) * theta)
        return TMV, T, R
    else
        return NaN, NaN, NaN
    end
end

end
