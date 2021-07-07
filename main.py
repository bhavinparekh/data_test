import pandas as pd


def clean_data(file):
    # read csv and pass pandas data frame
    df = pd.read_csv(file, header=None)

    # rename column name
    df.rename(columns={0: 'userId',
                       1: 'itemId',
                       2: 'rating',
                       3: 'timestamp'},
              inplace=True)
    print('step1')

    # write lookupuser.csv : userId,userIdAsInteger
    lookupuser = pd.DataFrame(df.userId.unique())
    lookupuser['userIdAsInteger'] = lookupuser.index
    lookupuser.rename(columns={0: 'userId'},
                      inplace=True)
    lookupuser.to_csv('lookupuser.csv', index=False, header=None)
    print('step2')

    # write lookup_product.csv : itemId,itemIdAsInteger
    lookup_product = pd.DataFrame(df.itemId.unique())
    lookup_product['itemIdAsInteger'] = lookup_product.index
    lookup_product.rename(columns={0: 'itemId'},
                          inplace=True)
    lookup_product.to_csv('lookup_product.csv', index=False, header=None)
    print('step3')

    # merge df and lookupuser dataframe base on userId
    df = df.merge(lookupuser[['userIdAsInteger', 'userId']], left_on='userId', right_on='userId', how='left')

    # merge df and lookup_product dataframe base on itemId
    df = df.merge(lookup_product[['itemIdAsInteger', 'itemId']], left_on='itemId', right_on='itemId', how='left')
    print('step4')

    # use group by on df for Sum of the ratings for the couple user/product
    aggratings = df[['userIdAsInteger', 'itemIdAsInteger', 'rating']].groupby(
        ['userIdAsInteger', 'itemIdAsInteger']).sum().reset_index()
    aggratings.to_csv('aggratings.csv', index=False, header=None)
    print('finish')
    return

if __name__ == "__main__":
    clean_data('data/xag.csv')
   #clean_data('https://storage.googleapis.com/ebap-data/technical-test/data-engineer/xag.csv')

