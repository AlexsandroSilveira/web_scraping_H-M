#==================================== IMPORTS =============================================
import os
import re
import sqlite3
import logging
import requests
import numpy as np
import pandas as pd


from bs4        import BeautifulSoup
from datetime   import datetime
from sqlalchemy import create_engine


#================================== DATA COLLECTION =======================================
def data_collection( url, headers):

    # Request to URL
    page = requests.get( url, headers=headers )

    # Beautiful Soup object
    soup = BeautifulSoup(page.text, 'html.parser')

    #======================== Product data ===============================
    products = soup.find('ul', class_='products-listing small')
    product_list = products.find_all( 'article', class_='hm-product-item')

    # product id
    product_id = [p.get( 'data-articlecode' )for p in product_list]

    # product category
    product_category = [p.get( 'data-category' )for p in product_list]

    # product name
    product_list = products.find_all( 'a', class_='link')
    product_name = [p.get_text() for p in product_list]

    # product price
    product_list = products.find_all( 'span', class_='price regular')
    product_price = [p.get_text() for p in product_list]
    
    # Dataframe
    data = pd.DataFrame([product_id, product_category, product_name, product_price]).T
    data.columns = ['product_id', 'product_category', 'product_name', 'product_price']

    return data

#============================= DATA COLLECTION BY PRODUCT==================================
def data_collection_by_product( data, headers ):

    # Empty Dataframe
    df_details = pd.DataFrame()

    #unique columns for all products
    aux = []

    cols = ['Art. No.', 'Care instructions', 'Composition', 'Concept', 'Description', 'Fit', 'Imported', 'Material',
            'More sustainable materials', 'Nice to know', 'Size', 'color_id', 'messages.clothingStyle',
            'messages.garmentLength', 'messages.waistRise', 'style_id']

    df_pattern = pd.DataFrame( columns=cols )

    for i in range( len(data) ):
        # API Request
        url = 'https://www2.hm.com/en_us/productpage.' + data.loc[i, 'product_id'] + '.html'
        
        logger.debug( 'Product: %s', url)

        page = requests.get( url, headers=headers )

        #Beautiful Soup
        soup = BeautifulSoup( page.text, 'html.parser')

        #=================================Color Name===============================================
        product_list = soup.find_all('a', {'class':['filter-option miniature', 'filter-option miniature active']} )
        color_name = [p.get('data-color') for p in product_list]

        # Product ID
        product_id = [p.get('data-articlecode') for p in product_list]

        df_color = pd.DataFrame([product_id, color_name]).T
        df_color.columns = ['product_id', 'color_name']

        for j in range(len(df_color)):
            # API Request
            url = 'https://www2.hm.com/en_us/productpage.' + df_color.loc[j, 'product_id'] + '.html'
            
            logger.debug( 'Color: %s', url)

            page = requests.get( url, headers=headers )

            #Beautiful Soup
            soup = BeautifulSoup( page.text, 'html.parser')
            
            #=============================== Product Name =====================================
            product_name_list = soup.find_all( 'h1', class_='primary product-item-headline')
            product_name = product_name_list[0].get_text()
            
            #=============================== Product Price =====================================
            product_price = soup.find_all( 'div', class_='primary-row product-item-price')
            product_price = re.findall(r'\d+\.?\d+', product_price[0].get_text())[0]
            
            #=================================Composition===============================================
            product_composition_list = soup.find_all('div', class_ = 'details-attributes-list-item')
            product_composition = [list(filter (None, p.get_text().split( '\n' ) ) ) for p in product_composition_list] 

            #Rename dataframe
            df_composition = pd.DataFrame(product_composition).T
            df_composition.columns = df_composition.iloc[0]
        
            # Delete First Row
            df_composition = df_composition.iloc[1:].fillna( method='ffill' )
            
            # Remove pocket line, pocket, shell and lining
            df_composition['Composition'] = df_composition['Composition'].replace( 'Pocket lining: ','', regex=True)
            df_composition['Composition'] = df_composition['Composition'].replace( 'Shell: ','', regex=True)
            df_composition['Composition'] = df_composition['Composition'].replace( 'Lining: ','', regex=True)
            df_composition['Composition'] = df_composition['Composition'].replace( 'Pocket: ','', regex=True)

            # Garantee the same number of columns
            df_composition = pd.concat( [df_pattern, df_composition], axis=0 )
            
            # Rename columns
            df_composition.columns = ['product_id', 'Care instructions', 'composition',
                                    'Concept', 'Description', 'fit', 'Imported', 'Material',
                                    'More sustainable materials', 'Nice to know', 'size', 
                                    'color_id', 'messages.clothingStyle',
                                    'messages.garmentLength', 'messages.waistRise', 'style_id']
            df_composition['product_name'] = product_name
            df_composition['product_price'] = product_price
            
            # Keep new columns if it show
            aux = aux + df_composition.columns.tolist()

            # Merge data color + composition
            data_SKU = pd.merge(df_composition, df_color, how='left', on='product_id')
                    
            # All details products
            df_details = pd.concat( [df_details, data_SKU], axis=0)
            
    # Join Showroom data + details
    df_details['style_id'] = df_details['product_id'].apply(lambda x: x[:-3])
    df_details['color_id'] = df_details['product_id'].apply(lambda x: x[-3:])

    # scrapy datetime
    df_details['scrapy_datetime'] = datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

    # Delete colummns and reset index
    df_details = df_details.drop(columns=['messages.garmentLength', 'messages.waistRise', 'Care instructions',
                            'More sustainable materials','Material', 'Description','Imported', 
                            'messages.clothingStyle','Concept', 'Nice to know'])
    
    return df_details

#======================================= DATA CLEANING=====================================
def data_cleaning ( data_product ):
    #product id
    data_raw = data_product.dropna( subset=['product_id'])

    #product name
    data_raw['product_name'] = data_raw['product_name'].str.replace( '\n', '' )
    data_raw['product_name'] = data_raw['product_name'].str.replace( '\t', '' )
    data_raw['product_name'] = data_raw['product_name'].str.replace( '  ', '' )
    data_raw['product_name'] = data_raw['product_name'].str.replace( ' ', '_' ).str.lower()

    #product price
    data_raw['product_price'] = data_raw['product_price'].astype(float) 

    #color name
    data_raw['color_name'] = data_raw['color_name'].str.replace( ' ', '_' ).str.lower()

    #fit
    data_raw['fit'] = data_raw['fit'].apply( lambda x: x.replace(' ', '_').lower())

    # # Size Number
    # data_raw['size_number'] = data_raw['size'].apply(lambda x: re.search('\d{3}cm', x ).group(0) if pd.notnull( x ) else x)
    # data_raw['size_number'] = data_raw['size_number'].apply( lambda x: re.search( '\d+', x).group(0) if pd.notnull( x ) else x)

    # # Size Model
    # data_raw['size_model'] = data_raw['size'].str.extract( '(\d+/\\d+)')

    #============================ composition ====================================
    # Brake composition
    df = data_raw['composition'].str.split(',', expand=True).reset_index( drop=True )

    # Empty dataframe with index of data_raw
    df_ref = pd.DataFrame(index=np.arange(len(data_raw)))

    #------------------------------ Cotton ---------------------------------------
    #Collect cotton in rows
    df_cotton_0 = df.loc[df[0].str.contains( 'Cotton', na=True), 0]
    df_cotton_0.name = 'cotton'
    df_cotton_1 = df.loc[df[1].str.contains( 'Cotton', na=True), 1]
    df_cotton_1.name = 'cotton'

    # Combine rows
    df_cotton = df_cotton_0.combine_first ( df_cotton_1 )

    df_ref = pd.concat([df_ref, df_cotton], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    #----------------------------- Polyester -------------------------------------- 
    #Collect polyester in rows
    df_polyester_0 = df.loc[df[0].str.contains('Polyester', na=True), 0]
    df_polyester_0.name = 'polyester'
    df_polyester_1 = df.loc[df[1].str.contains('Polyester', na=True), 1]
    df_polyester_1.name = 'polyester'

    # Combine rows
    df_polyester = df_polyester_0.combine_first ( df_polyester_1 )

    df_ref = pd.concat([df_ref, df_polyester], axis=1 )
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]
    #df_ref['polyester'] = df_ref['polyester'].fillna('Polyester 0%')
                        
    #------------------------------ Spandex ---------------------------------------
    #Collect spandex in rows
    df_spandex_1 = df.loc[df[1].str.contains('Spandex', na=True), 1]
    df_spandex_1.name = 'spandex'
    df_spandex_2 = df.loc[df[2].str.contains('Spandex', na=True), 2]
    df_spandex_2.name = 'spandex'

    # Combine rows
    df_spandex = df_spandex_1.combine_first ( df_spandex_2 )

    df_ref = pd.concat([df_ref,df_spandex], axis=1 )
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    #------------------------ Elastomultiester -------------------------------------
    #Collect elastomultiester in rows
    df_elastomultiester = df.loc[df[1].str.contains('Elastomultiester', na=True), 1]
    df_elastomultiester.name = 'elastomultiester'

    df_ref = pd.concat([df_ref,df_elastomultiester], axis=1 )
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # join of combine with product_id
    df_aux = pd.concat( [data_raw['product_id'].reset_index(drop=True), df_ref], axis=1 )

    #Format composition data
    df_aux['cotton'] = df_aux['cotton'].apply(lambda x: int(re.search( '\d+', x).group(0))/100 if pd.notnull( x ) else x )
    df_aux['polyester'] = df_aux['polyester'].apply(lambda x: int(re.search( '\d+', x).group(0))/100 if pd.notnull( x ) else x )
    df_aux['spandex'] = df_aux['spandex'].apply(lambda x: int(re.search( '\d+', x).group(0))/100 if pd.notnull( x ) else x )

    # Final Join
    df_aux = df_aux.groupby( 'product_id' ).max().reset_index().fillna( 0 )
    data = pd.merge( data_raw, df_aux, on='product_id', how='left' )

    # Drop columns
    data = data.drop(columns = ['composition', 'size'])

    # Drop duplicates
    data = data.drop_duplicates()
    data = data.reset_index(drop=True)

    return data

# ===================================DATA INSERT===========================================
def data_insert ( data ):
    data_insert = data[[
        'product_id',
        'style_id',
        'color_id',
        'product_name',
        'color_name',
        'fit',
        'product_price',
        'cotton',
        'polyester',
        'spandex',
        'elastomultiester',
        'scrapy_datetime'
]]
    # Create database connection
    conn = create_engine( 'sqlite:///database_hm.sqlite', echo=False)

    # Data insert
    data_insert.to_sql( 'vitrine', con=conn, if_exists='append',index=False)

    return None


if __name__ == '__main__':
    # logging
    path = 'logs'

    if not os.path.exists( path + 'Logs'):
        os.makedirs( path + 'Logs')

    logging.basicConfig(
        filename = path + 'Logs/webscraping_hm.log',
        level = logging.DEBUG,
        format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt = '%Y-%m-%d %H:%M%S'
        )

    logger = logging.getLogger( 'webscraping_hm' )


    # parameters
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5)AppleWebKit/605.1.15 (KHTML, like Gecko)Version/12.1.1 Safari/605.1.15'}
    # URL
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'

    # data collection
    data = data_collection( url, headers )
    logger.info( 'data collect done' )


    # data collection by product
    data_product = data_collection_by_product ( data, headers )
    logger.info( 'data collection by product done' )


    # data cleaning
    data_product_cleaned = data_cleaning ( data_product )
    logger.info( 'data cleaned done' )


    # data insertion
    data_insert( data_product_cleaned )
    logger.info( 'data insertion done' )
