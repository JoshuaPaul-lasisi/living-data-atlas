from etl.utils import save_data

dummy = {'helo':'world'}
save_data(dummy, 'data/raw/testsource/dummy.json')