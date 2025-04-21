import xml.etree.ElementTree as ET
from datetime import datetime

def validate_xml(xml_file, xsd_file):
    """Валидация XML по XSD схеме (требуется установка lxml)"""
    try:
        from lxml import etree
        xmlschema = etree.XMLSchema(etree.parse(xsd_file))
        xml_doc = etree.parse(xml_file)
        if xmlschema.validate(xml_doc):
            print("XML документ валиден по XSD схеме.")
            return True
        else:
            print("XML документ НЕ валиден по XSD схеме:")
            print(xmlschema.error_log)
            return False
    except ImportError:
        print("Для валидации требуется установить библиотеку lxml")
        return False
    except Exception as e:
        print(f"Ошибка при валидации: {e}")
        return False

def process_library(xml_file):
    """Обработка XML файла библиотеки"""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Вывод всех книг
        print("\nСписок всех книг в библиотеке:")
        total_price = 0
        book_count = 0
        
        for book in root.findall('book'):
            book_id = book.get('id')
            title = book.find('title').text
            author = book.find('author').text
            year = book.find('year').text
            genre = book.find('genre').text
            price = float(book.find('price').text)
            
            print(f"\nID: {book_id}")
            print(f"Название: {title}")
            print(f"Автор: {author}")
            print(f"Год: {year}")
            print(f"Жанр: {genre}")
            print(f"Цена: {price:.2f}")
            
            total_price += price
            book_count += 1
        
        # Вычисление средней цены
        if book_count > 0:
            avg_price = total_price / book_count
            print(f"\nСредняя цена книги: {avg_price:.2f}")
        
        # Фильтрация книг
        filter_genre = input("\nВведите жанр для фильтрации (или нажмите Enter чтобы пропустить): ")
        if filter_genre:
            filtered_books = [b for b in root.findall('book') 
                            if b.find('genre').text.lower() == filter_genre.lower()]
            print(f"\nНайдено {len(filtered_books)} книг в жанре '{filter_genre}':")
            for book in filtered_books:
                print(f"- {book.find('title').text} ({book.find('year').text})")
        
        filter_year = input("\nВведите год для фильтрации (или нажмите Enter чтобы пропустить): ")
        if filter_year:
            try:
                year = int(filter_year)
                filtered_books = [b for b in root.findall('book') 
                                if int(b.find('year').text) == year]
                print(f"\nНайдено {len(filtered_books)} книг, опубликованных в {year} году:")
                for book in filtered_books:
                    print(f"- {book.find('title').text} ({book.find('genre').text})")
            except ValueError:
                print("Ошибка: введите корректный год")
    
    except ET.ParseError as e:
        print(f"Ошибка при разборе XML: {e}")
    except FileNotFoundError:
        print(f"Файл {xml_file} не найден")
    except Exception as e:
        print(f"Неизвестная ошибка: {e}")

if __name__ == "__main__":
    xml_file = "library.xml"
    xsd_file = "library.xsd"
    
    # Валидация XML
    validate_xml(xml_file, xsd_file)
    
    # Обработка XML
    process_library(xml_file)