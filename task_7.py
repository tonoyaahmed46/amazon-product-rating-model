from import_statements import *
  
def task_7(data_io, train_data, test_data):
    
    dt = M.regression.DecisionTreeRegressor(maxDepth = 5)
    dt.setFeaturesCol("features")
    dt.setLabelCol("overall")
    model_dt = dt.fit(train_data)
    preds = model_dt.transform(test_data)
    
    evaluator = M.evaluation.RegressionEvaluator()
    evaluator.setPredictionCol("prediction")
    evaluator.setLabelCol("overall")
    
    rmse_final = evaluator.evaluate(preds, {evaluator.metricName: "rmse"})

    # -------------------------------------------------------------------------
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None
    }
    # Modify res:
    
    res['test_rmse'] = rmse_final

    # -------------------------------------------------------------------------
    data_io.save(res, 'task_7')
    return res