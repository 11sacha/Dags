import os
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup

FilePath = os.path.dirname(__file__)
print(FilePath)
os.chdir(FilePath)
os.chdir('../')
ProjectPath = os.getcwd()
print(ProjectPath)
FilesFolderPath = os.path.join(ProjectPath, r'Files')
print(FilesFolderPath)

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
}

def get_amazon_books(numb_books):

    files = ['BooksFile.csv']

    for file in files:
        try:
            os.remove(os.path.join(FilesFolderPath, file))
            print(f'Se elimino un archivo antiguo con el nombre de {file}')
        except:
            print('No se encontraron archivos viejos..')

    base_url = f"https://www.amazon.com/s?k=data+engineering+books"

    books = []
    seen_titles = set()

    page = 1

    while len(books) < numb_books:
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:

            print('Inicio webscrapping..')

            soup = BeautifulSoup(response.content, "html.parser")

            book_containers = soup.find_all("div", {"class": "s-result-item"})

            for book in book_containers:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})

                if title and author and price and rating:
                    book_title = title.text.strip()

                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                        })

            page += 1
        else:
            print("Failed to retrieve the page")
            break

    count = len(books)
    print(f'Encontre {count} libros')

    books = books[:numb_books]

    books_df = pd.DataFrame(books)

    books_df.drop_duplicates(subset='Title', inplace=True)

    print('Guardando archivo..')
    file_name = 'BooksFile.csv'
    books_df.to_csv(os.path.join(FilesFolderPath, file_name))
    print(f'Archivo guardado en {FilesFolderPath}.')

    #ti.xcom_push(key='book_data', value=books_df.to_dict('records'))


def run_process():
    get_amazon_books(50)

#run_process()



