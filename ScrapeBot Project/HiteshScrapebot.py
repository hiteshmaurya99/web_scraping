#Install packages form requirements file using pip install

import asyncio
import nest_asyncio
import datetime
import requests
import pandas as pd
from requests_html import AsyncHTMLSession
import bs4
import aiohttp

nest_asyncio.apply()

async def fetch_data(url):
    # Create an async HTML session
    session = AsyncHTMLSession()
    content_list = []
    Category_list = []

    i = 1
    now = True
    while now:
        try:
            # Make a request and render JavaScript
            response = await session.get(url + str(i), headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'})
            await response.html.arender(sleep=2)  # Adjust the sleep time as needed

            # Extract content after JavaScript execution
            page_content = response.html.html
            soup = bs4.BeautifulSoup(page_content, 'html.parser')
            # Extract category
            category = soup.select('h2')
            category_list = []
            for catg in category:
                category_list.append(catg.text)
            Category_list.extend(category_list[::2][:-1])    
            
            urls = soup.select('.jet-engine-listing-overlay-wrap')
            if urls:
                for url_of in urls:
                    content_list.append(url_of['data-url'])
                #print('Page :', i)
                i += 1
            else:
                now = False

        except Exception as e:
            print(f"An error occurred: {e}")
            break

    return content_list, Category_list


async def extract_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            page_content = await response.text()
            soup = bs4.BeautifulSoup(page_content, 'html.parser')

            # Extract Page URL
            page_url = url

            # Extract Name and Reward
            name_reward = [w.text.strip() for w in soup.select('h2')[:2]]

            # Extract Associated Organization
            associate_org_h2_top = soup.select_one('#Rewards-Organizations-Links > div > h2')
            associate_org_text_top = associate_org_h2_top.text.strip() if associate_org_h2_top else None

            associate_org_div_bottom = soup.select_one('#reward-fields > div > div.elementor-element.elementor-element-b7c9ae6.dc-has-condition.dc-condition-empty.elementor-widget.elementor-widget-text-editor > div')
            associate_org_text_bottom = associate_org_div_bottom.get_text(strip=True).replace('\xa0', ' ').replace(';', ',') if associate_org_div_bottom else None

            associate_org_text = associate_org_text_top if associate_org_text_top else associate_org_text_bottom

            # Handle null values
            if not associate_org_text_top and not associate_org_text_bottom:
                associate_org_text = 'null'

            # Extract Associated Location
            loc_span = soup.find('span', class_='jet-listing-dynamic-terms__link')
            location = loc_span.text.strip() if loc_span else 'null'

            # Extract About
            about_section_div = soup.find('div', class_='elementor-element elementor-element-52b1d20 elementor-widget elementor-widget-theme-post-content')
            about_text = about_section_div.text.strip() if about_section_div else 'null'

            # Extract Images
            pics = []
            profile_div = soup.find_all('div', id='gallery-1')
            if profile_div:
                for paragraph in profile_div:
                    profile_links = paragraph.find_all('a')
                    profile_pic_urls = [link['href'] for link in profile_links]
                    pics.append(profile_pic_urls)

            # Handle null value for pics
            else:
                pics = 'null'

            # Extract Date Of Birth
            dob_div = soup.find('div', class_='elementor-element elementor-element-9a896ea dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor')
            dob_text = dob_div.text.strip() if dob_div else 'null'

            # Set formatted_date to None if dob_text is empty
            if not dob_text:
                formatted_date = 'null'
            else:
                try:
                    # Try parsing the DOB text as it is
                    date_object = datetime.datetime.strptime(dob_text, '%B %d, %Y')
                    formatted_date = date_object.strftime('%Y-%m-%d')
                except ValueError:
                    # If parsing fails, assign the DOB text as formatted date
                    formatted_date = dob_text

    return {
        'Page URL': page_url,
        'Title': name_reward[0],
        'Reward Amount': name_reward[1],
        'Associated Organization(s)': associate_org_text,
        'Associated Location(s)': location,
        'About': about_text,
        'Images URL(s)': pics,
        'Date Of Birth': formatted_date
    }

async def main():
    url = 'https://rewardsforjustice.net/index/?jsf=jet-engine:rewards-grid&tax=crime-category:1070%2C1071%2C1073%2C1072%2C1074&pagenum='
    all_urls, category_list = await fetch_data(url)
    tasks = [extract_data(url) for url in all_urls]
    results = await asyncio.gather(*tasks)

    # Convert results to DataFrame
    df = pd.DataFrame(results)

    # Save DataFrame to JSON and XLSX
    df['Category'] = category_list  # Add category column
    df.to_json('output.json', orient='records', indent=4)
    df.to_excel('output.xlsx', index=False)

if __name__ == "__main__":
    nest_asyncio.apply()
    print("Starting script...")
    loop = asyncio.get_event_loop()
    print("Running main function...")
    loop.run_until_complete(main())
    print("Script execution completed.")
