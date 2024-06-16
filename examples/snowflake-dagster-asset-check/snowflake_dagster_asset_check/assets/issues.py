from dagster import asset
from dagster_snowflake import SnowflakeResource


@asset
def issues_history(
    snowflake: SnowflakeResource,
) -> None:
    """
    creates the table of fake issue data

    | issue_id | new_state   | updated_at |
    |----------|-------------|------------|
    | 1        | open        | 2021-01-01 |
    | 1        | in progress | 2021-01-02 |
    | 2        | open        | 2021-01-02 |
    | 1        | closed      | 2021-01-03 |
    | 2        | in progress | 2021-01-03 |
    | 2        | closed      | 2021-01-05 |
    """
    with snowflake.get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE OR REPLACE TABLE issues_by_day (
                issue_id INT,
                new_state STRING,
                updated_at DATE
            );

            """
        )

        cur.execute(
            """
            INSERT INTO issues_by_day (issue_id, new_state, updated_at) VALUES
              (1, 'open', '2021-01-01'),
              (1, 'in progress', '2021-01-02'),
              (2, 'open', '2021-01-02'),
              (1, 'closed', '2021-01-03'),
              (2, 'in progress', '2021-01-03'),
              (2, 'closed', '2021-01-05')
            ;
            """
        )


@asset(
    deps=["issues_history"],
)
def issues_by_day(
    snowflake: SnowflakeResource,
) -> None:
    """
    uses the issues by day table to create a table of issue state history

    | date       | open | in progress | closed |
    | ---------- | ---- | ----------- | ------ |
    | 2021-01-01 | 1    | 0           | 0      |
    | 2021-01-02 | 1    | 1           | 0      |
    | 2021-01-03 | 0    | 1           | 1      |
    | 2021-01-04 | 0    | 1           | 1      |
    | 2021-01-05 | 0    | 0           | 2      |

    updating the table in place if it already exists
    """
    with snowflake.get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE OR REPLACE TABLE issue_state_history AS
            SELECT
                updated_at AS date,
                SUM(CASE WHEN new_state = 'open' THEN 1 ELSE 0 END) AS open,
                SUM(CASE WHEN new_state = 'in progress' THEN 1 ELSE 0 END) AS in_progress,
                SUM(CASE WHEN new_state = 'closed' THEN 1 ELSE 0 END) AS closed
            FROM issues_by_day
            GROUP BY updated_at
            ORDER BY updated_at;
            """
        )
