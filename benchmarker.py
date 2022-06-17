import ept
import pdal
import timeit

def get_ept_python():

    url = 's3://mapdwell-temp-storage/entwine_builds/ma_26919'
    bounds = ept.Bounds(
        312045, #xmin
        4671729, #ymin
        -999999, #zmin
        312145, #xmax
        4671829, #ymax
        9999999 #zmax
    )

    query = ept.EPT(url, bounds=bounds)
    las = query.as_laspy()
    print(las)


def get_pdal():
    bbox = f'([312045, 312145], [4671729, 4671829], [-999999, 9999999])'
    pipeline = pdal.Reader.ept(
        filename='s3://mapdwell-temp-storage/entwine_builds/ma_26919/ept.json',
        bounds=bbox
    ).pipeline()
    pipeline.execute()
    data = pipeline.arrays[0]
    print(len(data))


timer = timeit.Timer(get_ept_python)
results = timer.repeat(repeat=20, number=1)
print()
print('ept_python timings')
print(f'\t min: {min(results)}')
print(f'\t avg: {sum(results) / len(results)}')
print(f'\t max: {max(results)}')


timer = timeit.Timer(get_pdal)
results = timer.repeat(repeat=20, number=1)
print()
print('ept_python timings')
print(f'\t min: {min(results)}')
print(f'\t avg: {sum(results) / len(results)}')
print(f'\t max: {max(results)}')