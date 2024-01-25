import sys
import numpy as np
import pandas as pd
from scipy import stats

OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value:  {more_searches_p:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value:  {more_instr_searches_p:.3g}'
)


def main():
    searchdata_file = sys.argv[1]
    searchdata_file = "searches.json"
    searches = pd.read_json(searchdata_file, orient='records', lines=True)

    #all ppl
    #Did users search more/less?
    odd = searches[searches['uid']%2==1]
    even = searches[searches['uid']%2==0]
    more_searches_p = stats.mannwhitneyu(odd['search_count'], even['search_count']).pvalue
    #Did more/less users use the search feature?
    odd_searched = odd[odd['search_count']>0]['search_count'].count()
    odd_nosearch = odd[odd['search_count']==0]['search_count'].count()
    even_searched = even[even['search_count']>0]['search_count'].count()
    even_nosearch = even[even['search_count']==0]['search_count'].count()
    contingency = [[odd_searched, odd_nosearch], [even_searched, even_nosearch]]
    chi2, more_users_p, dof, expected = stats.chi2_contingency(contingency) 

    #instructors
    #Did instructors search more/less?
    instructors_odd = odd[odd['is_instructor']==True]
    instructors_even = even[even['is_instructor']==True]
    more_instr_searches_p = stats.mannwhitneyu(instructors_odd['search_count'], instructors_even['search_count']).pvalue
    #Did more/less instructors use the search feature?
    instructors_odd_searched = instructors_odd[instructors_odd['search_count']>0]['search_count'].count()
    instructors_odd_nosearch = instructors_odd[instructors_odd['search_count']==0]['search_count'].count()
    instructors_even_searched = instructors_even[instructors_even['search_count']>0]['search_count'].count()
    instructors_even_nosearch = instructors_even[instructors_even['search_count']==0]['search_count'].count()
    contingency_instr = [[instructors_odd_searched, instructors_odd_nosearch], [instructors_even_searched, instructors_even_nosearch]]
    chi2_instr, more_instr_p, dof_instr, expected_instr = stats.chi2_contingency(contingency_instr)

    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p=more_users_p,
        more_searches_p=more_searches_p,
        more_instr_p=more_instr_p,
        more_instr_searches_p=more_instr_searches_p,
    ))


if __name__ == '__main__':
    main()
