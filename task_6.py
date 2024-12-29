from import_statements import *

def task_6(data_io, product_processed_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    category_column = 'category'
    # Outputs:
    categoryIndex_column = 'categoryIndex'
    categoryOneHot_column = 'categoryOneHot'
    categoryPCA_column = 'categoryPCA'
    # -------------------------------------------------------------------------    

    string_indexer = M.feature.StringIndexer(inputCol=category_column, outputCol=categoryIndex_column)
    model_str = string_indexer.fit(product_processed_data)
    transformed_df_str = model_str.transform(product_processed_data)
    
    ohe = M.feature.OneHotEncoder(dropLast = False)
    ohe.setInputCols([categoryIndex_column])
    ohe.setOutputCols([categoryOneHot_column])
    model_one_hot = ohe.fit(transformed_df_str)
    transformed_df_ohe = model_one_hot.transform(transformed_df_str)
    
    pca = M.feature.PCA(k = 15, inputCol=categoryOneHot_column)
    pca.setOutputCol(categoryPCA_column)
    model_pca = pca.fit(transformed_df_ohe)
    transformed_df_pca = model_pca.transform(transformed_df_ohe)


    one_hot_mean = (transformed_df_pca.select(M.stat.Summarizer.mean(transformed_df_pca.categoryOneHot)).head()[0])
    pca_mean = (transformed_df_pca.select(M.stat.Summarizer.mean(transformed_df_pca.categoryPCA)).head()[0])

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'meanVector_categoryOneHot': [None, ],
        'meanVector_categoryPCA': [None, ]
    }
    # Modify res:
    
    res['count_total'] = transformed_df_pca.count()
    res['meanVector_categoryOneHot'] = one_hot_mean
    res['meanVector_categoryPCA'] = pca_mean

    # -------------------------------------------------------------------------
    data_io.save(res, 'task_6')
    return res