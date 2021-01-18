module DirectionalChange

using Dates
using DataFrames


function init(data::DataFrame, dc_offset::AbstractVector{<:Number}, DC_Algo::Symbol, down_ind=Vector{Number}(undef, 1)::AbstractVector{<:Number})
    column_names = names(data)
    if ncol(data) > 2 || column_names[1] != "Timestamp" || typeof(column_names[1]) == Float64
        error("Data is not aligned with the required structure!
                    Module expects only two columns for the DataFrame: Timestamp and Price")
    end
    rename!(data, [:Timestamp, :Price])

    # defining the type of algorith that is used in this run
    if DC_Algo == :TSFDC && length(dc_offset) == 2
        sort!(dc_offset, rev=true)
        Algo = :TSFDC
    elseif DC_Algo == :IDBA
        if !isassigned(down_ind, 1)
            error("In case of IDBA algorithm down_ind vector can not be empty")
        end
        sort!(dc_offset)
        Algo = :IDBA
    elseif DC_Algo == :Book
        sort!(dc_offset)
        Algo = :Book
    else
        error("Type of algorithm and/or number of items in dc_offset vector are not correct!")
    end
    return data, dc_offset, Algo, down_ind
end

function pct_change(input::AbstractVector{<:Number}, period::Int=1)
    res = @view(input[(period + 1):end]) ./ @view(input[1:(end - period)]) .- 1
    [fill(missing, period); res]
end

function prepare(data::DataFrame, dc_offset::AbstractVector{<:Number}, Algo::Symbol, down_ind::AbstractVector{<:Number})
        # preparing dataframe for getting fit
    insertcols!(data, :pct_change => pct_change(data.Price))
    dropmissing!(data, :pct_change)
    if Algo == :TSFDC
        last_dc_offset = last(dc_offset)
    end
    for current_offset_value in dc_offset
        insertcols!(data, "Event_$(current_offset_value)" => "")
        if Algo == :Book
            insertcols!(data, "Event_$(current_offset_value)_TMV" => NaN)
            insertcols!(data, "Event_$(current_offset_value)_T" => NaN)
            insertcols!(data, "Event_$(current_offset_value)_R" => NaN)
        elseif Algo == :IDBA
            for down_index in down_ind
                insertcols!(data, "Event_$(current_offset_value)_OSV_down_ind_$(down_index)" => NaN)
            end
        elseif Algo == :TSFDC && current_offset_value == last_dc_offset
            insertcols!(data, "Event_$(current_offset_value)_BBTheta" => false)
            insertcols!(data, "Event_$(current_offset_value)_OSV" => NaN)
        end
    end
    return data, dc_offset, Algo, down_ind
end

function fit(data::DataFrame, dc_offset::AbstractVector{<:Number}, Algo::Symbol, down_ind::AbstractVector{<:Number})
    rows = Tables.namedtupleiterator(data)
    dc_offset_length = length(dc_offset)
    DC_event = repeat(["init"], dc_offset_length)
    DC_highest_price = repeat([data[1,:Price]], dc_offset_length)
    DC_lowest_price = repeat([data[1,:Price]], dc_offset_length)
    DC_highest_price_index = repeat([data[1, :Timestamp]], dc_offset_length)
    DC_lowest_price_index = repeat([data[1, :Timestamp]], dc_offset_length)
    last_dc_offset = last(dc_offset)
    if Algo == :IDBA
        IDBA_last_downtrend = Vector{DataFrame}(undef, length(dc_offset))
    end
    function fit_implementation(row, index, offset_value, current_offset_column, last_round=false)
        if DC_event[index] == "downtrend" || DC_event[index] == "init"
            if row.Price >= (DC_lowest_price[index] * (1 + offset_value))
                DC_event[index] = "uptrend"
                data[(data.Timestamp .== row.Timestamp), current_offset_column] = ["Up"]
                check_null_value = data[(data.Timestamp .== DC_lowest_price_index[index]), current_offset_column]
                isempty(check_null_value[1]) ?
                    data[data.Timestamp .== DC_lowest_price_index[index], current_offset_column] = ["DXP"] :
                    data[data.Timestamp .== DC_lowest_price_index[index], current_offset_column] = ["Down+DXP"]
                if Algo == :IDBA
                    IDBA_last_downtrend[index] = DataFrame()
                end
                if Algo == :Book
                    TMV, T, R = calculate_TMV_T_R(data, current_offset_column, DC_lowest_price_index[index], DC_lowest_price[index], offset_value, "UXP")
                    if !isnan(TMV)
                        data[data.Timestamp .== DC_lowest_price_index[index], "$(current_offset_column)_TMV"] = [TMV]
                    end
                    if !isnan(T)
                        data[data.Timestamp .== DC_lowest_price_index[index], "$(current_offset_column)_T"] = [T]
                    end
                    if !isnan(R)
                        data[data.Timestamp .== DC_lowest_price_index[index], "$(current_offset_column)_R"] = [R]
                    end
                end
                # Dealing with TSFDC algorith
                if Algo == :TSFDC && last_round
                    dc_current_lowest_price = data[(data.Timestamp .== DC_lowest_price_index[index]), "Event_$(dc_offset[1])"]
                    dc_current_lowest_price == ["DXP"] || dc_current_lowest_price == ["Down+DXP"] ?
                            data[(data.Timestamp .== row.Timestamp), "$(current_offset_column)_BBTheta"] = [true] :
                            data[(data.Timestamp .== row.Timestamp), "$(current_offset_column)_BBTheta"] = [false]
                    osv_value = OSV(data, row.Price, DC_lowest_price_index[index], dc_offset[1])
                    if !isnan(osv_value)
                        data[(data.Timestamp .== row.Timestamp), "$(current_offset_column)_OSV"] = [osv_value]
                    end
                end
                DC_highest_price[index] = row.Price
                DC_highest_price_index[index] = row.Timestamp
            end
            if Algo == :IDBA && isassigned(IDBA_last_downtrend, index) && !isempty(IDBA_last_downtrend[index])
                last_downtrend_time = IDBA_last_downtrend[index].Timestamp[1]
                last_downtrend_price = IDBA_last_downtrend[index].Price[1]
                if row.Timestamp > last_downtrend_time
                    OSV = ((row.Price - last_downtrend_price) / last_downtrend_price) / offset_value
                    for down_index in down_ind
                        if OSV > down_index
                            data[(data.Timestamp .== row.Timestamp), "$(current_offset_column)_OSV_down_ind_$(down_index)"] = [OSV]
                        end
                    end
                end
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
                if Algo == :IDBA
                    IDBA_last_downtrend[index] = data[(data.Timestamp .== row.Timestamp), [:Timestamp, :Price]]
                end
                check_null_value = data[(data.Timestamp .== DC_highest_price_index[index]), current_offset_column]
                isempty(check_null_value[1]) ?
                    data[(data.Timestamp .== DC_highest_price_index[index]), current_offset_column] = ["UXP"] :
                    data[(data.Timestamp .== DC_highest_price_index[index]), current_offset_column] = ["Up+UXP"]
                if Algo == :Book
                    TMV, T, R = calculate_TMV_T_R(data, current_offset_column, DC_highest_price_index[index], DC_highest_price[index], offset_value, "DXP")
                    if !isnan(TMV)
                        data[data.Timestamp .== DC_highest_price_index[index], "$(current_offset_column)_TMV"] = [TMV]
                    end
                    if !isnan(T)
                        data[data.Timestamp .== DC_highest_price_index[index], "$(current_offset_column)_T"] = [T]
                    end
                    if !isnan(R)
                        data[data.Timestamp .== DC_highest_price_index[index], "$(current_offset_column)_R"] = [R]
                    end
                end
                # Dealing with TSFDC algorithm
                if Algo == :TSFDC && last_round
                    dc_current_highest_price = data[(data.Timestamp .== DC_highest_price_index[index]), "Event_$(dc_offset[1])"]
                    dc_current_highest_price == ["UXP"] || dc_current_highest_price == ["Up+UXP"] ?
                             data[(data.Timestamp .== row.Timestamp), "$(current_offset_column)_BBTheta"] = [true] :
                             data[(data.Timestamp .== row.Timestamp), "$(current_offset_column)_BBTheta"] = [false]
                    osv_value = OSV(data, row.Price, DC_highest_price_index[index], dc_offset[1])
                    if !isnan(osv_value)
                        data[(data.Timestamp) .== row.Timestamp, "$(current_offset_column)_OSV"] = [osv_value]
                    end
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
    # To imporve performance, loop structure sperated for TSFDC and the rest of the Algos
    # TSFDC relies on BTheta to be run first, So it needs to run in order.
    # But the other two algos dont have this requirement and we can run all thetas in every row iteration.
    if Algo == :TSFDC
        for (index, offset_value) in enumerate(dc_offset)
            current_offset_column = "Event_$(offset_value)"
            last_round = offset_value == last_dc_offset
            for row in rows
                fit_implementation(row, index, offset_value, current_offset_column, last_round)
            end
        end
    elseif Algo == :IDBA || Algo == :Book
        for row in rows
            for (index, offset_value) in enumerate(dc_offset)
                current_offset_column = "Event_$(offset_value)"
                fit_implementation(row, index, offset_value, current_offset_column)
            end
        end
    end
    return data
end

# Calculating OSV value as an independent variable used for prediction according to the paper
function OSV(data, price, STheta_extreme_index, BTheta)
    BTheta_column = "Event_$BTheta"
    BTheta_rows = data[isless.(data.Timestamp, STheta_extreme_index),:]
    BTheta_rows = BTheta_rows[.!isempty.(BTheta_rows[!,BTheta_column]),:]
    rows = reverse(Tables.rows(BTheta_rows))
    if !isempty(rows)
        return calculate_OSV_value(price, BTheta, rows)
    else
        return NaN
    end
end

function calculate_OSV_value(price, BTheta, rows)
    BTheta_column = "Event_$BTheta"
    direction = ["Down", "Down+DXP", "Up", "Up+UXP"]
    for row in eachrow(rows)
        if row[1][BTheta_column] in direction
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
        alternate_direction = "Down+DXP"
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
        R = (abs(TMV) / T) * theta
        return TMV, T, R
    else
        return NaN, NaN, NaN
    end
end

end
