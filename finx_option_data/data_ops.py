import sqlalchemy as sa
from typing import Optional, Dict


def create_upsert_method(meta, extra_update_fields: Optional[Dict[str, str]]):
    """
    Create upsert method that satisfied the pandas's to_sql API.
    """

    def method(table, conn, keys, data_iter):
        # select table that data is being inserted to (from pandas's context)
        sql_table = sa.Table(table.name, meta, autoload=True)

        # list of dictionaries {col_name: value} of data to insert
        values_to_insert = [dict(zip(keys, data)) for data in data_iter]

        # create insert statement using postgresql dialect.
        # For other dialects, please refer to https://docs.sqlalchemy.org/en/14/dialects/
        insert_stmt = sa.dialects.postgresql.insert(sql_table, values_to_insert)

        # create update statement for excluded fields on conflict
        update_stmt = {exc_k.key: exc_k for exc_k in insert_stmt.excluded}
        if extra_update_fields:
            update_stmt.update(extra_update_fields)

        # create upsert statement.
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=sql_table.primary_key.columns,  # index elements are primary keys of a table
            set_=update_stmt,  # the SET part of an INSERT statement
        )

        # execute upsert statement
        conn.execute(upsert_stmt)

    return method

def create_insert_do_nothing_method(meta):
    def method(table, conn, keys, data_iter):
        # select table that data is being inserted to (from pandas's context)
        sql_table = sa.Table(table.name, meta, autoload=True)

        # list of dictionaries {col_name: value} of data to insert
        values_to_insert = [dict(zip(keys, data)) for data in data_iter]

        # create insert statement using postgresql dialect.
        # For other dialects, please refer to https://docs.sqlalchemy.org/en/14/dialects/
        insert_stmt = sa.dialects.postgresql.insert(sql_table, values_to_insert).on_conflict_do_nothing()

        # execute insert statement
        conn.execute(insert_stmt)

    return method



def df_upsert(df, engine, model):
    meta = sa.MetaData(engine)
    upsert_method = create_upsert_method(meta, extra_update_fields={})
    df.to_sql(
        model.__tablename__,
        engine,
        schema="public",
        index=False,
        if_exists="append",
        chunksize=200,  # it's recommended to insert data in chunks
        method=upsert_method,
    )

def df_insert_do_nothing(df, engine, model):
    meta = sa.MetaData(engine)
    insert_method = create_insert_do_nothing_method(meta)
    df.to_sql(
        model.__tablename__,
        engine,
        schema="public",
        index=False,
        if_exists="append",
        chunksize=200,  # it's recommended to insert data in chunks
        method=insert_method,
    )