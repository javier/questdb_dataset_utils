from questdb.ingress import Sender, IngressError, TimestampNanos
from git import Repo
import psycopg2


from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import sys

def connect_postgres(host: str = '127.0.0.1', user: str = 'admin', pwd: str = 'quest', port: int = 8812, dbname: str = 'qdb'):
    try:
        conn = psycopg2.connect(f'user={user} password={pwd} host={host} port={port} dbname={dbname}')
        conn.autocommit = False

        return conn
    except psycopg2.Error as e:
        print(f'Had problem connecting with error {e}.')


def create_table(table_name):
    conn = connect_postgres()
    with conn.cursor() as cur:
            cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS  '{table_name}' (
                        committed_datetime TIMESTAMP,
                        author_name SYMBOL,
                        summary STRING,
                        size INT,
                        insertions INT,
                        deletions INT,
                        lines INT,
                        files INT
                        ) timestamp (committed_datetime) PARTITION BY MONTH WAL DEDUP UPSERT KEYS(committed_datetime, author_name);

                        """
                        )
    conn.commit()




def insert_commits(commits, table_name):
    try:
        with Sender('localhost', 9009) as sender:
            for commit in commits:
                summary = (commit.summary[:75] + '..') if len(commit.summary) > 75 else commit.summary
                sender.row(
                    table_name,
                    symbols={'author_name': commit.author.name},
                    columns={'summary': summary, 'size': commit.size,
                             'insertions': commit.stats.total['insertions'], 'deletions': commit.stats.total['deletions'],
                             'lines': commit.stats.total['lines'], 'files': commit.stats.total['files'] },
                    at=TimestampNanos.from_datetime(commit.committed_datetime)
                    )

            sender.flush()
    except IngressError as e:
        print(f'Got error: {e}', flush=True)
    except Exception as e:
        print(f'Got error: {e}', flush=True)
    t2 = datetime.now()


if __name__ == '__main__':
    repo_dir = None
    if len(sys.argv) > 1:
        repo_dir = sys.argv[1]
    else:
        repo_dir = "."

    print(repo_dir)
    repo = Repo(repo_dir)
    repo_name = repo.working_dir.split('/')[-1]
    table_name = f'{repo_name}_commits'
    create_table(table_name)
    commits = repo.iter_commits("master", max_count=10000000)
    insert_commits(commits, table_name)










