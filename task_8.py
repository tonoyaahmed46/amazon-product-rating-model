from import_statements import *

def task_8(data_io, train_data, test_data):
    
    train, validation = train_data.randomSplit([0.75, 0.25])
    max_depths = [5, 7, 9, 12]
    
    rmse_dic = {}
    for i in range(len(max_depths)): 
        depth = max_depths[i] 
        dt = M.regression.DecisionTreeRegressor(maxDepth = depth)
        dt.setFeaturesCol("features")
        dt.setLabelCol("overall")
        model_dt = dt.fit(train)
        preds = model_dt.transform(validation)

        evaluator = M.evaluation.RegressionEvaluator()
        evaluator.setPredictionCol("prediction")
        evaluator.setLabelCol("overall")

        rmse_per_depth = evaluator.evaluate(preds, {evaluator.metricName: "rmse"})
        rmse_dic[rmse_per_depth] = (depth)
    
    rmse_list = list(rmse_dic.keys())
    
    lowest_rmse = min(rmse_list)
    best_depth = rmse_dic[lowest_rmse]
    
    dt = M.regression.DecisionTreeRegressor(maxDepth = best_depth)
    dt.setFeaturesCol("features")
    dt.setLabelCol("overall")
    model_dt = dt.fit(train_data)
    preds = model_dt.transform(test_data)

    evaluator = M.evaluation.RegressionEvaluator()
    evaluator.setPredictionCol("prediction")
    evaluator.setLabelCol("overall")

    best_rmse = evaluator.evaluate(preds, {evaluator.metricName: "rmse"})
    
    # -------------------------------------------------------------------------
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None,
        'valid_rmse_depth_5': None,
        'valid_rmse_depth_7': None,
        'valid_rmse_depth_9': None,
        'valid_rmse_depth_12': None,
    }
    # Modify res:
    
    res['test_rmse'] = best_rmse
    res['valid_rmse_depth_5'] = rmse_list[0]
    res['valid_rmse_depth_7'] = rmse_list[1]
    res['valid_rmse_depth_9'] = rmse_list[2]
    res['valid_rmse_depth_12'] = rmse_list[3]
        
    # -------------------------------------------------------------------------

    data_io.save(res, 'task_8')
    return res