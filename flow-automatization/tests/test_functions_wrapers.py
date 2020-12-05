from functions_wrapers import get_best_model_path
import pytest
from cassandra.cluster import Cluster
import pandas as pd


@pytest.fixture(scope='session')
def pandas_factory_fixture():
    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    return pandas_factory


# Parametryzowany test funkcji sprawdzający, czy funkcja get_best_model poprawnie odczytuję dane z bazy danych
@pytest.mark.parametrize('model_name, stat', [
    ("'RF_vel'",
     'min'
     )])
def test_get_best_model_path(model_name, stat, pandas_factory_fixture):

    path = get_best_model_path(model_name, stat)
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect("models")
    session.row_factory = pandas_factory_fixture
    session.default_fetch_size = None
    query_count = "select count(*) from models_statistics where model_path = '%s'" \
                  " ALLOW FILTERING;"
    query_count = query_count % path
    count = session.execute(query_count, timeout=None)._current_rows.iloc[0]['count']
    session.shutdown()
    cluster.shutdown()
    assert count > 0

