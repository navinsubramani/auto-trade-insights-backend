import pandas as pd


def get_index_metadata(INDEX):
    """
    This function reads the index_metadata.csv file and index_company_details.csv file and returns the index list with the weightage of each company in the index and the sector of the company.
    """

    # Read the dataframe from the CSV file
    index_list = pd.read_csv('index_metadata.csv')

    # Get specific columns and with non NAN rows from the dataframe
    index_list = index_list[[INDEX+" Company", INDEX+" Weight"]]
    index_list = index_list.dropna()
    # Convert the "x %" weight strings into number
    index_list[INDEX+" Weight"] = index_list[INDEX+" Weight"].str.replace("%", "").astype(float)

    # Read the Sector details of the companies
    sector_list = pd.read_csv('index_company_details.csv')
    sector_list = sector_list[["Company", "Updated Sector"]]

    # For each index in the index_list, get the corresponding sector from the sector_list
    for i in range(len(index_list)):
        company = index_list.iloc[i, 0]
        sector = sector_list[sector_list["Company"] == company]["Updated Sector"].values[0]
        index_list.at[i, "Sector"] = sector

    # print(index_list)

    stocks_total_weightage = round(index_list[INDEX+" Weight"].sum(), 4)
    cash_total_weightage = 100 - stocks_total_weightage

    # print("Total weightage of stocks: ", stocks_total_weightage)
    # print("Total weightage of cash: ", cash_total_weightage)
    
    return index_list, stocks_total_weightage, cash_total_weightage