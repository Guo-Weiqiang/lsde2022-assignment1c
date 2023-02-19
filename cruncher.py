import sys
import duckdb

con = duckdb.connect()
con.execute("SET TimeZone='UTC'")


def cruncher(datadir, a1, a2, a3, a4, d1, d2): 
    con.execute("CREATE TEMP TABLE interests2 AS \
                SELECT * FROM interests \
                WHERE interest = %d OR interest = %d OR interest = %d OR interest = %d" % (a1, a2, a3, a4))
    # focus
    con.execute("CREATE TEMP TABLE focus AS \
                SELECT personId, interest, CASE WHEN interest != %d then TRUE ELSE FALSE END AS nofan FROM interests2" % a1)
    # score
    con.execute("CREATE TEMP TABLE scores AS \
                SELECT person.personId, person.bday, count(person.personId) AS score, min(focus.nofan) AS nofan FROM person, focus \
                WHERE person.personId = focus.personId \
                GROUP BY person.personId, person.bday")

    # candas
    con.execute("CREATE TEMP TABLE cands AS\
                SELECT * FROM scores WHERE score > 0 AND nofan = TRUE \
                AND %d <= bday\
                AND bday <= %d" % (d1, d2))

    # pairs
    con.execute("CREATE TABLE pairs AS \
                SELECT cands.personId, knows.friendId, cands.score FROM cands, knows \
                WHERE cands.personId = knows.personId")


    # fanlocs
    con.execute("CREATE TABLE fanlocs AS \
                SELECT personId AS friendId FROM scores \
                WHERE nofan = FALSE")
    # results
    con.execute("SELECT score, personId AS p, pairs.friendId as f FROM pairs, fanlocs\
                WHERE pairs.friendId = fanlocs.friendId\
                ORDER BY score DESC, p ASC, f ASC")
    re = con.fetchall()

    con.execute("DROP TABLE interests2")
    con.execute("DROP TABLE focus")
    con.execute("DROP TABLE scores")
    con.execute("DROP TABLE cands")
    con.execute("DROP TABLE pairs")
    con.execute("DROP TABLE fanlocs")   

    return re


def run_cruncher(datadir, query_file_path, results_file_path, number_of_queries = 10):
    query_file = open(query_file_path, 'r')
    results_file = open(results_file_path, 'w')

    con.execute("CREATE TABLE person AS \
                SELECT * FROM '%s'" % (datadir + "/person_parquet/*.parquet"))
    con.execute("CREATE TABLE knows AS \
                SELECT * FROM '%s' " % (datadir + "/knows_parquet/*.parquet"))
    con.execute("CREATE TABLE interests AS \
                SELECT * FROM '%s' " % (datadir + "/interest_parquet/*.parquet"))
    # con.execute("ALTER TABLE person ALTER birthday TYPE DATE")
    i = 0
    for line in query_file.readlines():
        i = i + 1
        q = line.strip().split("|")

        # parse rows like: 1|1989|1990|5183|1749|2015-04-09|2015-05-09
        qid = int(q[0])
        a1 = int(q[1])
        a2 = int(q[2])
        a3 = int(q[3])
        a4 = int(q[4])
        d1 = 100 * (int(q[5][5:7])) + int(q[5][8:10])
        d2 = 100 * (int(q[6][5:7])) + int(q[6][8:10])

        print(f"Processing Q{qid}")
        result = cruncher(datadir, a1, a2, a3, a4, d1, d2)

        # write rows like: Query.id|TotalScore|P|F
        for row in result:
            # print("111")
            results_file.write(f"{qid}|{row[0]}|{row[1]}|{row[2]}\n")
        
        if i >= number_of_queries:
            break

    query_file.close()
    results_file.close()

    # return the last result
    return result


def main():
    if len(sys.argv) < 4:
        print("Usage: cruncher.py [datadir] [query file] [results file]")
        sys.exit()

    datadir = sys.argv[1]
    query_file_path = sys.argv[2]
    results_file_path = sys.argv[3]

    run_cruncher(datadir, query_file_path, results_file_path)


if __name__ == "__main__":
    main()

