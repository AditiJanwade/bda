import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.neighbors import KNeighborsRegressor
from sklearn.metrics import r2_score, mean_squared_error

def load_data(path):
    data = pd.read_csv(path)
    X = data[['GRE Score', 'TOEFL Score', 'University Rating', 'SOP', 'LOR', 'CGPA', 'Research']]
    y = data['Chance of Admit']
    return X, y

def build_pipeline(model):
    return Pipeline([
        ('scaler', StandardScaler()),
        ('model', model)
    ])

def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    return r2, rmse

def main():
    X, y = load_data("Synthetic_Graduate_Admissions.csv")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    models = {
        "Linear Regression": LinearRegression(),
        "Decision Tree": DecisionTreeRegressor(random_state=42),
        "Random Forest": RandomForestRegressor(random_state=42),
        "Support Vector Regressor": SVR(kernel='rbf'),
        "KNN Regressor": KNeighborsRegressor(n_neighbors=5)
    }

    results = []
    for name, model in models.items():
        pipe = build_pipeline(model)
        pipe.fit(X_train, y_train)
        r2, rmse = evaluate_model(pipe, X_test, y_test)
        results.append({"Model": name, "R² Score": round(r2, 3), "RMSE": round(rmse, 3)})

    results_df = pd.DataFrame(results)
    print("\nModel Performance Summary:")
    print(results_df.to_string(index=False))

    sample = pd.DataFrame([[320, 110, 4, 4.5, 4.0, 9.0, 1]],
                          columns=['GRE Score', 'TOEFL Score', 'University Rating', 'SOP', 'LOR', 'CGPA', 'Research'])
    best_model = build_pipeline(RandomForestRegressor(random_state=42))
    best_model.fit(X, y)
    pred = best_model.predict(sample)
    print(f"\nPredicted Chance of Admission: {pred[0]*100:.2f}%")

if __name__ == "__main__":
    main()
