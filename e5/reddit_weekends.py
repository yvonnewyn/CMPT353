import sys
import numpy as np
import pandas as pd
from scipy import stats

def get_id(date):
    id = str(date.isocalendar().year)+"-"+str(date.isocalendar().week)
    return id

OUTPUT_TEMPLATE = (
    "Initial T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann-Whitney U-test p-value: {utest_p:.3g}"
)


def main():
    reddit_counts = sys.argv[1]
    counts = pd.read_json(reddit_counts, lines=True)

    counts = counts[counts['subreddit']=='canada']
    counts = counts[(counts['date'].dt.year == 2012) | (counts['date'].dt.year == 2013)] 

    weekday = counts[(counts['date'].dt.dayofweek != 5) & (counts['date'].dt.dayofweek != 6)]
    weekday.reset_index()
    weekend = counts[(counts['date'].dt.dayofweek == 5) | (counts['date'].dt.dayofweek == 6)]
    weekend.reset_index()

    p = stats.ttest_ind(weekday['comment_count'], weekend['comment_count']).pvalue
    weekday_normal = stats.normaltest(weekday['comment_count']).pvalue
    weekend_normal = stats.normaltest(weekend['comment_count']).pvalue
    equal_var = stats.levene(weekday['comment_count'], weekend['comment_count']).pvalue

    #fix 1: transform
    weekday2 = weekday.copy(deep=True)
    weekday2['comment_count'] = np.sqrt(weekday2['comment_count'])
    weekend2 = weekend.copy(deep=True)
    weekend2['comment_count'] = np.sqrt(weekend2['comment_count'])
    weekday2_normal = stats.normaltest(weekday2['comment_count']).pvalue
    weekend2_normal = stats.normaltest(weekend2['comment_count']).pvalue
    equal_var2 = stats.levene(weekday2['comment_count'], weekend2['comment_count']).pvalue

    #fix 2: CLT
    weekday3 = weekday.copy(deep=True)
    weekday3['id'] = weekday3['date'].apply(get_id)
    weekday3 = weekday3.groupby('id').mean()

    weekend3 = weekend.copy(deep=True)
    weekend3['id'] = weekend3['date'].apply(get_id)
    weekend3 = weekend3.groupby('id').mean()
    weekday3_normal = stats.normaltest(weekday3['comment_count']).pvalue
    weekend3_normal = stats.normaltest(weekend3['comment_count']).pvalue
    equal_var3 = stats.levene(weekday3['comment_count'], weekend3['comment_count']).pvalue
    p3 = stats.ttest_ind(weekday3['comment_count'], weekend3['comment_count']).pvalue

    #fix 3: mann whitney
    utest = stats.mannwhitneyu(weekday['comment_count'], weekend['comment_count']).pvalue

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p = p,
        initial_weekday_normality_p = weekday_normal,
        initial_weekend_normality_p = weekend_normal,
        initial_levene_p = equal_var,
        transformed_weekday_normality_p = weekday2_normal,
        transformed_weekend_normality_p = weekend2_normal,
        transformed_levene_p = equal_var2,
        weekly_weekday_normality_p = weekday3_normal,
        weekly_weekend_normality_p = weekend3_normal,
        weekly_levene_p = equal_var3,
        weekly_ttest_p = p3,
        utest_p = utest,
    )) 





if __name__ == '__main__':
    main()