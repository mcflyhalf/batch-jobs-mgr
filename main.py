from batchers import create_batches, print_batch, update_batch_info, gen_report

# from products import ProdFreqDB
from models import engine, Product, ProdConfig, get_session, ProdFreqDB
import datetime
import random
import os


datafile = "db_large.csv"
# datafile = "db.csv"
report_file = "results/analysis_{}days_{}.csv"
num_days = 100

strategies = ["deterministic", "random", "stochastic"]
for strategy in strategies:
    ProdConfig(engine, datafile)
    session = get_session()
    pfd = ProdFreqDB(session)

    # for small db
    # DAILY_BATCHES = 6
    # MAX_BATCH_SIZE = 2

    # for large db
    DAILY_BATCHES = 40
    MAX_BATCH_SIZE = 6

    # Batch creation strategy (stochastic|deterministic|random)
    # strategy = "stochastic"

    # remember to delete all files in results dir before starting
    curr_datetime = datetime.datetime.now()
    # Create batches for the next num_days days
    for i in range(num_days):
        b = create_batches(DAILY_BATCHES, MAX_BATCH_SIZE, pfd, curr_datetime, strategy = strategy)
        # Will cause problems on windows
        batch_record_file = "results/batches_day{}.txt".format(str(i + 1))
        with open(batch_record_file, "w") as outfile:
            for b_num, batch in enumerate(b):
                outfile.write(print_batch(b_num + 1, batch))
        curr_datetime += datetime.timedelta(days=1)

    runs = {prod.name: 0 for prod in pfd.all_products}

    # breakpoint()
    with os.scandir("results") as results_dir:
        for result in results_dir:
            if result.is_file() and "batches_day" in result.path:
                update_batch_info(result.path, runs)

    # Create csv
    gen_report(report_file.format(num_days,strategy), pfd, runs, num_days)
