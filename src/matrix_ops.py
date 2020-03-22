from scipy import sparse
from sklearn.metrics.pairwise import linear_kernel
import numpy as np
import pandas as pd

def get_cosine_similarities(tfidf, tfidf_USA, thres = 0.4, save_path = '../processed/cosine_similarities.npz', batch_size=100):
    cosine_similarities = sparse.csr_matrix((tfidf.shape[0],tfidf_USA.shape[0]))
    values = np.zeros(cosine_similarities.shape[0])
    indexes = np.zeros(cosine_similarities.shape[0])

    for batch_start in range(0, tfidf.shape[0], batch_size):
        batch_end = min(batch_start+batch_size, tfidf.shape[0])
        cos_sim = linear_kernel(tfidf[batch_start:batch_end], tfidf_USA, dense_output=True)
        for i in range(batch_start,batch_end):
            index = np.argmax(cos_sim[i-batch_start])
            value = cos_sim[i-batch_start,index]
            indexes[i] = index
            values[i] = value

        #decrease number of elements in the sparse matrix
        cos_sim[cos_sim<thres] = 0
        cos_sim_sparse = sparse.csr_matrix(cos_sim)
        cosine_similarities[batch_start:batch_end] = cos_sim_sparse
        if batch_start % 1000 == 0:
            print(f'{batch_start} of {tfidf.shape[0]} documents are calculated')
    sparse.save_npz(save_path, cosine_similarities)
    np.save('../processed/max_similarities.npy', values)

    return cosine_similarities, values
    # TODO: also save and load values and indexes


def company_name_to_usa_df_mapping(companies, usa_df):
    comp_df = pd.DataFrame(companies, columns=['company_name'])
    comp_df = comp_df.reset_index()
    comp_df = comp_df.set_index('company_name')
    comp_df.columns = ['company_name_index']

    # set to -1 to get an index also for empty company names
    comp_df.loc[''] = -1

    rec_indexes = comp_df.loc[usa_df.clean_recipient_name.fillna('')].values.flatten()
    par_indexes = comp_df.loc[usa_df.clean_recipient_parent_name.fillna('')].values.flatten()
    bus_indexes = comp_df.loc[usa_df.clean_recipient_doing_business_as_name.fillna('')].values.flatten()

    one_hot = sparse.lil_matrix((usa_df.shape[0], comp_df.shape[0]))
    one_hot_parent = sparse.lil_matrix((usa_df.shape[0], comp_df.shape[0]))

    one_hot[range(usa_df.shape[0]), rec_indexes] = True
    one_hot[range(usa_df.shape[0]), bus_indexes] = True
    one_hot_parent[range(usa_df.shape[0]), par_indexes] = True


    one_hot_row = one_hot.T.tocsr()
    one_hot_parent_row = one_hot_parent.T.tocsr()
    return one_hot_row, one_hot_parent_row

def get_best_candidates(chair_df, usa_df, cosine_similarities, companies, zip_bonus = 0.1, state_bonus=0.1,
                        address_bonus= 0.3):
    columns = list(chair_df.columns) + list(usa_df.columns) + ['cos_sim', 'score','matched_by_parent_name']
    best_matches = pd.DataFrame(columns=columns, index=range(chair_df.shape[0]))

    comp_name_to_usa_mapping, parent_comp_name_to_usa_mapping = company_name_to_usa_df_mapping(companies, usa_df)
    
    for i in range(chair_df.shape[0]):
        chair_candidate = chair_df.iloc[i]
        index = cosine_similarities[i].indices

        if i % 1000 == 0:
            print(f'{i} of {chair_df.shape[0]} documents are calculated')

        #if i == 1000:
        #    break

        if index.shape[0] == 0:
            continue


        usa_indexes = comp_name_to_usa_mapping[index].indices
        parent_usa_indexes = parent_comp_name_to_usa_mapping[index].indices

        expanded_cos_sims = []
        # get child company cos sims
        for j in range(len(cosine_similarities[i].indices)):
            selection = comp_name_to_usa_mapping[index[j]]
            expanded_cos_sims += [cosine_similarities[i].data[j]] * len(selection.indices)
        
        # get parent company cos sims
        for j in range(len(cosine_similarities[i].indices)):
            selection = parent_comp_name_to_usa_mapping[index[j]]
            expanded_cos_sims += [cosine_similarities[i].data[j]] * len(selection.indices)

        child_candidates = usa_df.iloc[usa_indexes].copy()
        child_candidates['matched_by_parent_name'] = False
        
        parent_candidates = usa_df.iloc[parent_usa_indexes].copy()
        parent_candidates['matched_by_parent_name'] = True
        
        candidates = pd.concat([child_candidates,parent_candidates], ignore_index=True)
        candidates['cos_sim'] = expanded_cos_sims
        #candidates['cos_sim'] = candidates.groupby(candidates.index)['cos_sim'].max()
        candidates = candidates.drop_duplicates()
        is_zip_bonus = (candidates.recipient_zip_code == chair_candidate.addzip) * ~candidates.matched_by_parent_name
        candidates['zip_bonus'] = zip_bonus * is_zip_bonus
        is_state_bonus = (candidates.recipient_state_fixed == chair_candidate.state_fixed) * ~candidates.matched_by_parent_name
        candidates['state_bonus'] = state_bonus * is_state_bonus
        is_address_bonus = (candidates.recipient_address_line_fixed == 
                            chair_candidate.add_fixed) * ~candidates.matched_by_parent_name
        candidates['address_bonus'] = address_bonus * is_address_bonus

        candidates['total_bonus'] = candidates.zip_bonus + candidates.state_bonus + candidates.address_bonus

        both_have_state = np.logical_and(~pd.isna(candidates.recipient_state_fixed),
                                                                         ~pd.isna(chair_candidate.state_fixed))

        no_match_condition = np.logical_and(np.logical_and(~candidates.matched_by_parent_name,
                                                           ~is_state_bonus),both_have_state)

        candidates['total_bonus'] += -(candidates['total_bonus']+candidates.cos_sim)*no_match_condition

        candidates['score'] = candidates.cos_sim + candidates.total_bonus
        # TODO: take the one with most data/count
        candidate = candidates.loc[candidates.score.astype('float').idxmax()]
        best_matches.iloc[i] = pd.concat([chair_candidate, candidate], axis=0)

    return best_matches