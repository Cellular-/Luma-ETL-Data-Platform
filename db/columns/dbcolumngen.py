import os

def columns_for_update(filename, target, src):
    with open(filename, 'r') as cols:
        line = cols.readline().replace('\n', '')
        new_cols = list(map(lambda col: col.strip(), line.split(',')))

    cols_w_aliases = [f"tgt.{col} = src.{col}" for col in new_cols]
    return ',\n'.join(cols_w_aliases)

def columns_for_insert(filename):
    with open(filename, 'r') as cols:
        line = cols.readline().replace('\n', '')
        new_cols = list(map(lambda col: col.strip(), line.split(',')))

    return new_cols

print(
    columns_for_update(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cols.txt'),
        'tgt',
        'src'
    )
)

print(
    ',\n'.join(list(map(
    lambda col: f'src.{col}',
    columns_for_insert(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cols.txt')
        )
    )))
)