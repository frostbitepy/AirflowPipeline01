from google.cloud import bigquery
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.impute import SimpleImputer
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
import logging

def load_data(table_id,uri):
    client = bigquery.Client()

    table = client.get_table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
    )
    job_config.field_delimiter = ","
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

def preprocess_data(project_id, dataset_id, input_table_id, output_table_id):
    client = bigquery.Client(project=project_id)
    input_query = f"SELECT * FROM `{project_id}.{dataset_id}.{input_table_id}`"
    df = client.query(input_query).to_dataframe()

    # Preprocesar los datos
    df = df.drop(columns=['Conductivity', 'Trihalomethanes'])
    imputer = SimpleImputer(strategy='mean')
    df_imputed = pd.DataFrame(imputer.fit_transform(df), columns=df.columns)

    # Vaciar la tabla de salida y cargar los datos preprocesados
    client.delete_table(f'{project_id}.{dataset_id}.{output_table_id}', not_found_ok=True)
    client.load_table_from_dataframe(df_imputed, f'{project_id}.{dataset_id}.{output_table_id}').result()

       
def ml_training_SupportVectorMachine_bigquery(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    final = client.query(query).to_dataframe()

    # Dividir el DataFrame final en X e y
    X = final.iloc[:, 1:]
    y = final.iloc[:, 0]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    clf = SVC(kernel = 'linear', random_state = 0)
    clf.fit(X_train, y_train)
  
    y_pred = clf.predict(X_test)
  
    acc = accuracy_score(y_test, y_pred)
    return {'model_accuracy': acc}
    
def ml_training_KNearestNeighbors_bigquery(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    final = client.query(query).to_dataframe()

    # Dividir el DataFrame final en X e y
    X = final.iloc[:, 1:]
    y = final.iloc[:, 0]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    clf = KNeighborsClassifier(n_neighbors = 5, metric = 'minkowski', p = 2)
    clf.fit(X_train, y_train)
  
    y_pred = clf.predict(X_test)
  
    acc = accuracy_score(y_test, y_pred)
    return {'model_accuracy': acc}
    
