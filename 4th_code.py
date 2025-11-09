import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import networkx as nx

OUTDIR = "outputs"
os.makedirs(OUTDIR, exist_ok=True)

def savefig(fig, name, dpi=150):
    path = os.path.join(OUTDIR, name)
    fig.savefig(path, bbox_inches="tight", dpi=dpi)
    plt.close(fig)
    print("Saved:", path)

from sklearn.datasets import load_iris
iris = load_iris(as_frame=True)
df_iris = iris.frame
df_iris['species'] = df_iris.target.apply(lambda x: iris.target_names[x])

np.random.seed(42)
df_adult = pd.DataFrame({
    "age": np.random.randint(18, 70, 200),
    "hours_per_week": np.random.randint(20, 60, 200),
    "education_num": np.random.randint(1, 16, 200),
    "income": np.random.choice([">50K", "<=50K"], 200),
    "workclass": np.random.choice(["Private", "Self-emp", "Gov"], 200),
    "year": np.random.choice(range(2000, 2020), 200)
})

fig = plt.figure(figsize=(7,4))
sns.histplot(df_adult['age'], bins=20, kde=True, color="skyblue")
plt.title("1D Distribution of Age (Adult Dataset)")
savefig(fig, "adult_1d_hist_age.png")

fig = plt.figure(figsize=(7,5))
sns.scatterplot(data=df_iris, x="sepal length (cm)", y="sepal width (cm)", hue="species", palette="Set2")
plt.title("2D Scatterplot of Iris Features")
savefig(fig, "iris_2d_scatter.png")

fig3d = px.scatter_3d(
    df_iris, 
    x="sepal length (cm)", 
    y="sepal width (cm)", 
    z="petal length (cm)", 
    color="species",
    title="3D Iris Visualization"
)
fig3d.write_image(os.path.join(OUTDIR, "iris_3d.png"))
print("Saved:", os.path.join(OUTDIR, "iris_3d.png"))

df_yearly = df_adult.groupby("year")["hours_per_week"].mean().reset_index()
fig = plt.figure(figsize=(8,4))
sns.lineplot(data=df_yearly, x="year", y="hours_per_week", marker="o")
plt.title("Temporal Trend of Hours per Week (Adult Dataset)")
savefig(fig, "adult_temporal_year_hours.png")

pair = sns.pairplot(df_iris, hue="species", diag_kind="kde")
pair.fig.suptitle("Multidimensional Visualization of Iris Dataset", y=1.02)
pair.savefig(os.path.join(OUTDIR, "iris_multidimensional_pairplot.png"))
plt.close("all")
print("Saved:", os.path.join(OUTDIR, "iris_multidimensional_pairplot.png"))

from scipy.cluster.hierarchy import dendrogram, linkage
X = df_iris[["sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)"]]
linked = linkage(X, method="ward")
fig = plt.figure(figsize=(10, 5))
dendrogram(linked, labels=df_iris['species'].values, leaf_rotation=90, leaf_font_size=8)
plt.title("Hierarchical Clustering Dendrogram (Iris Dataset)")
savefig(fig, "iris_dendrogram.png")

G = nx.Graph()
for wc in df_adult['workclass'].unique():
    for inc in df_adult['income'].unique():
        G.add_edge(wc, inc, weight=np.random.randint(1,10))

fig = plt.figure(figsize=(6,6))
pos = nx.spring_layout(G)
nx.draw(G, pos, with_labels=True, node_size=2000, node_color="lightgreen", font_size=12, width=2)
plt.title("Network Visualization of Workclass vs Income")
savefig(fig, "adult_network.png")

print(" All visualizations saved inside:", OUTDIR)
