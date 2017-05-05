import math

# compute the RSME and MAE
def main():
    files = ["part-m-0000" + str(i) for i in range(7)]
    rm_sum = 0
    mae_sum = 0
    n = 0
    for filename in files:
        with open(filename) as f:
            for line in f:
                # get rid of trailing newline
                line = line.strip()
                ratings = line.split("\t")[1]
                predicted, actual = [float(val) for val in ratings.split(",")]
                dif = predicted - actual
                mae_sum += abs(dif)
                rm_sum += dif ** 2
                n += 1

    print("MAE: {0:.4f}".format(mae_sum / n))
    print("RMSE: {0:.4f}".format(math.sqrt(rm_sum / n)))

if __name__ == '__main__':
    main()

