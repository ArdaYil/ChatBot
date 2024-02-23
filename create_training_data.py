import sqlite3
import pandas as pd

timeframes = ["2015-05"]

for timeframe in timeframes:
    connection = sqlite3.connect("{}.db".format(timeframe))
    cursor = connection.cursor()
    limit = 5000
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False

    while (cur_length == limit):
        dataframe = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix, limit), connection)
        last_unix = dataframe.tail(1)["unix"].values[0]
        cur_length = len(dataframe)

        if not test_done:
            with open("test.from", "a", encoding="utf8") as f:
                for content in dataframe["parent"].values:
                    f.write(content+ "\n") 

            with open("test.to", "a", encoding="utf8") as f:
                for content in dataframe["comment"].values:
                    f.write(content+ "\n") 

            test_done = True
        
        else:
            with open("train.from", "a", encoding="utf8") as f:
                for content in dataframe["parent"].values:
                    f.write(content+ "\n") 

            with open("train.to", "a", encoding="utf8") as f:
                for content in dataframe["comment"].values:
                    f.write(content+ "\n") 

        counter += 1

        if counter % 20 == 0:
            print(counter * limit, "rows completed so far")