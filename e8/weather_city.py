import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import StandardScaler


def main():

    labelled = pd.read_csv(sys.argv[1])
    unlabelled = pd.read_csv(sys.argv[2])
    X = labelled.drop(['city', 'year'], axis=1)
    y = labelled['city'].values

    X_train, X_valid, y_train, y_valid = train_test_split(X, y)

    model = make_pipeline(
        StandardScaler(),
        SVC(kernel='linear', C=0.1)

    )

    model.fit(X_train, y_train)
    print(model.score(X_valid, y_valid))

    X_unlabelled = unlabelled.drop(['city', 'year'], axis=1)
    y_predicted = model.predict(X_unlabelled)

    # output file
    pd.Series(y_predicted).to_csv(sys.argv[3], index=False, header=False)


if __name__ == '__main__':
    main()
