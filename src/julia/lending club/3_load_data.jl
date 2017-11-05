@time include("0_setup.jl")
using IterableTables, FastGroupBy

@everywhere import JuliaDB.extractarray

if !isdefined(:data)
    @time data = load("lc_juliadb")
end

# fields = fieldnames(eltype(data))
fields = names(data)
ftypes = eltype(data).parameters

xx = JuliaDB.extractarray(data, x -> x.loan_status)


numeric_cols = [fields[i] for i = 1:length(fields) if (ftypes[i] <: Number ||
            ftypes[i] <: Nullable{<:Number}) && !(fields[i] in [:id, :member_id, :dti_joint])];

const bad_statuses = ("Late (16-30 days)","Late (31-120 days)","Default","Charged Off")
good_loans = filter(data) do row
    !(row.loan_status in bad_statuses)
end
bad_loans = filter(x->x.loan_status in bad_statuses, data)

using Gadfly
import NullableArrays: dropnull

# Density plot for bad and good loans
plots = Gadfly.Plot[]

good_numbers = good_loans[:loan_amnt]
@which columns(good_loans,:loan_amnt)

for (name, g, b) in zip(numeric_cols, columns(good_loans), columns(bad_loans))
    g = NullableArrays.dropnull(g)
    b = NullableArrays.dropnull(b)
    p = plot(layer(x=g, Geom.density, Theme(default_color=colorant"green")),
             layer(x=b, Geom.density, Theme(default_color=colorant"red")),
              Guide.title(string(name)), Guide.ylabel("density"))
    push!(plots, p)
end

perm = randperm(length(data))
train_till = round(Int, length(data) * 3/4)

training_subidx = sort!(perm[1:train_till])
testing_subidx = sort!(perm[train_till+1:end]);

training_subset = data[training_subidx]
testing_subset = data[testing_subidx]

features_train = [revol_util_train int_rate_train mths_since_last_record_train annual_inc_joint_train total_rec_prncp_train all_util_train]

labels = collect(map(x->x in bad_statuses, data))

import NullableArrays: dropnull
findnonnulls(xs::Columns) = find(x->!any(map(isnull, x)), xs)

function input_matrix(table, fields)
    float_features = collect(values(table, fields))
    tmp = findnonnulls(float_features) # indices of the rows where all fields are non-null
    nzidxs = collect(keys(table,1)[tmp]) # corresponding indices in the table
    reduce(hcat, map(dropnull, columns(float_features[tmp]))), values(labels[nzidxs]) # matrix, label vector
end


training_matrix, train_labels = input_matrix(training_subset, numeric_features)
model = build_forest(train_labels, training_matrix, 3, 10, 0.8, 6)

#     0.512169 seconds (7.67 k allocations: 440.766 KiB)
#   Out[98]:
#   Ensemble of Decision Trees
#   Trees:      10
#   Avg Leaves: 41.8
#   Avg Depth:  8.0

f = open("  loanmodel.jls", "w")
serialize(f, model)
close(f)

features_test, test_labels = input_matrix(testing_subset, numeric_features)

@time predictions = mapslices(features_test, 2) do fs
    DecisionTree.apply_forest(model, fs)
end;

# Receiver Operating Characteristics curve
using ROC
curve = roc(convert(Vector{Float64}, predictions[:]),convert(BitArray{1}, test_labels))

# An ROC plot in Gadfly with data calculuated using ROC.jl

plot(layer(x=curve.FPR, y=curve.TPR, Geom.line),
     layer(x = linspace(0.0,1.0,101), y = linspace(0.0,1.0,101),
       Geom.line, Theme(default_color=colorant"red")), Guide.title("ROC"),
       Guide.xlabel("False Positive Rate"),Guide.ylabel("True Positive Rate"))

# Area Under Curve
AUC(curve)

# => 0.7291976225854385
