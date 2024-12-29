from import_statements import *

def task_5(data_io, product_processed_data, word_0, word_1, word_2):
    # -----------------------------Column names--------------------------------
    # Inputs:
    title_column = 'title'
    # Outputs:
    titleArray_column = 'titleArray'
    titleVector_column = 'titleVector'
    # -------------------------------------------------------------------------

    tokenizer = M.feature.Tokenizer(outputCol = titleArray_column)
    tokenizer.setInputCol(title_column)
    product_processed_data_output = tokenizer.transform(product_processed_data)

    word2Vec = M.feature.Word2Vec(minCount = 100, vectorSize = 16, seed = 102, numPartitions = 4, inputCol=titleArray_column, outputCol=titleVector_column)
    model = word2Vec.fit(product_processed_data_output)

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'size_vocabulary': None,
        'word_0_synonyms': [(None, None), ],
        'word_1_synonyms': [(None, None), ],
        'word_2_synonyms': [(None, None), ]
    }
    # Modify res:
    res['count_total'] = product_processed_data_output.count()
    res['size_vocabulary'] = model.getVectors().count()
    for name, word in zip(
        ['word_0_synonyms', 'word_1_synonyms', 'word_2_synonyms'],
        [word_0, word_1, word_2]
    ):
        res[name] = model.findSynonymsArray(word, 10)
    # -------------------------------------------------------------------------
    data_io.save(res, 'task_5')
    return res