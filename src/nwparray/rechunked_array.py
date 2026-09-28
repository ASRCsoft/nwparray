'''Create a dask array where the chunks contain multiple smaller arrays.
'''

import numpy as np
import dask.array as da
from itertools import product

# def recursively_stack(f, coords):
#     '''Run function `f` on each combination of coordinates, stacking the results
#     into a single array with coordinates `coords` + coordinates of the array
#     returned by `f`.
#     '''
#     n_coords = len(coords)
#     if n_coords == 1:
#         # stack the arrays from `f`
#         arrays = [ f(i) for i in coords[0] ]
#     elif n_coords > 1:
#         # stack the results from `recursively_stack`, and modify `f` to accept
#         # fewer arguments
#         arrays = []
#         for i in coords[0]:
#             def f_inner(*args):
#                 return f(i, *args)
#             arrays.append(recursively_stack(f_inner, coords[1:]))
#     else:
#         raise Exception("Coordinates are empty")
#     # stack the arrays


def stack_product(f, *coords):
    '''Run function `f` on each combination of coordinates, stacking the results
    into a single array with coordinates `coords` + coordinates of the array
    returned by `f`.
    '''
    inputs_shape = tuple(len(x) for x in coords)
    results = np.array([ f(*args) for args in product(*coords) ])
    # first dimension of results is len(product), rest are from the result of
    # `f`
    out_shape = inputs_shape + results.shape[1:]
    return results.reshape(out_shape)


def map_rechunked_blocks(f, f_shape, coords, chunk_size, f_dtype):
    '''Create a chunked array, similar to `map_blocks`, but with each chunk
    aggregating multiple `f` results
    '''
    inputs_shape = tuple(len(x) for x in coords)

    # compute and organize a chunk of `f` results
    def f_chunk(block_info=None):
        # Need the actual coordinate values represented by this block. It
        # depends on the chunk size and chunk indices
        chunk_ind = block_info[None]['array-location']
        # chunk_coords = [ coords[i][chunk_ind[i][0]:chunk_ind[i][1]] for i in range(len(coords)) ]
        chunk_coords = []
        for i in range(len(coords)):
            coords_i = coords[i]
            chunk_ind_i = chunk_ind[i]
            chunk_coords.append(coords_i[chunk_ind_i[0]:chunk_ind_i[1]])
        return stack_product(f, *chunk_coords)

    # get a tuple of chunk lengths in each dimension
    out_chunks = []
    for i in range(len(chunk_size)):
        n_chunks = int(np.ceil(inputs_shape[i] / chunk_size[i]))
        chunk_list = [chunk_size[i], ] * n_chunks
        chunk_rem = inputs_shape[i] % chunk_size[i]
        if chunk_rem:
            chunk_list[-1] = chunk_rem
        out_chunks.append(tuple(chunk_list))

    return da.map_blocks(f_chunk, dtype=f_dtype, chunks=tuple(out_chunks) + f_shape,
                         meta=np.array((), dtype=f_dtype))
