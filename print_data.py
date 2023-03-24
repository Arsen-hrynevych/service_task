import psycopg2


class DataBase:
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    def connect(self):
        self.connection = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        return results

    def close(self):
        self.cursor.close()
        self.connection.close()

    def print_data(self):
        results = cursor.fetchall()
        for row in results:
            region, total_population, largest_country, largest_population, smallest_country, smallest_population = row
            print(f"Region name: {region}")
            print(f"Total population: {total_population:,}")
            print(f"Largest country: {largest_country}")
            print(f"Population of largest country: {largest_population:,}")
            print(f"Smallest country: {smallest_country}")
            print(f"Population of smallest country: {smallest_population:,}")
            print("--------------------------------")


if __name__ == '__main__':
    db = DataBase(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="db"
    )
    db.connect()
    cursor = db.cursor
    cursor.execute("""
SELECT
    CASE
        WHEN Country IN ('China', 'India', 'Indonesia', 'Pakistan', 'Bangladesh', 'Japan', 'Philippines', 'Vietnam', 'Iran', 'Turkey', 'Thailand', 'Myanmar', 'South Korea') THEN 'Asia'
        WHEN Country IN ('United States', 'Brazil', 'Mexico', 'Canada') THEN 'Americas'
        WHEN Country IN ('Nigeria', 'Ethiopia', 'DR Congo', 'Tanzania', 'South Africa', 'Kenya', 'Sudan', 'Uganda', 'Angola', 'Mozambique', 'Ghana', 'Ivory Coast', 'Madagascar', 'Cameroon', 'Niger', 'Burkina Faso', 'Mali', 'Malawi', 'Zambia', 'Senegal', 'Chad', 'Somalia', 'Zimbabwe', 'Guinea', 'Rwanda', 'Benin', 'Burundi', 'South Sudan', 'Eritrea', 'Egypt', 'Algeria', 'Morocco', 'Uganda', 'Tunisia', 'Libya', 'Sierra Leone', 'Togo', 'Liberia', 'Namibia', 'Botswana', 'Lesotho', 'Gabon', 'Equatorial Guinea', 'Mauritania', 'Gambia', 'Guinea-Bissau', 'Comoros', 'Cape Verde', 'Sao Tome and Principe') THEN 'Africa'
        WHEN Country IN ('Russia', 'Germany', 'France', 'United Kingdom', 'Italy', 'Spain', 'Ukraine', 'Poland', 'Romania', 'Netherlands', 'Belgium', 'Czech Republic', 'Greece', 'Portugal', 'Sweden', 'Hungary', 'Belarus', 'Austria', 'Switzerland', 'Bulgaria', 'Denmark', 'Finland', 'Norway', 'Ireland', 'Croatia', 'Serbia', 'Slovakia', 'Latvia', 'Lithuania', 'Estonia', 'Cyprus', 'Iceland', 'Malta', 'Luxembourg', 'Georgia', 'Albania', 'Moldova', 'North Macedonia', 'Montenegro', 'Bosnia and Herzegovina', 'Kosovo') THEN 'Europe'
        WHEN Country IN ('Australia', 'New Zealand', 'Papua New Guinea', 'Fiji', 'Solomon Islands', 'Vanuatu', 'New Caledonia (France)', 'French Polynesia (France)', 'Samoa', 'Guam (United States)', 'Northern Mariana Islands (United States)', 'Cook Islands', 'Wallis and Futuna (France)', 'Tuvalu', 'Niue', 'Tokelau (New Zealand)', 'Christmas Island (Australia)', 'Norfolk Island (Australia)', 'Cocos (Keeling) Islands (Australia)', 'Pitcairn', 'Micronesia', 'Marshall Islands', 'Palau', 'Kiribati', 'Nauru') THEN 'Oceania'
        WHEN Country IN ('Saudi Arabia', 'Yemen', 'Oman', 'United Arab Emirates', 'Qatar', 'Kuwait', 'Bahrain') THEN 'Middle East'
        WHEN Country IN ('Israel', 'Palestine', 'Jordan', 'Lebanon', 'Syria', 'Iraq', 'Iran', 'Cyprus', 'Turkey') THEN 'Middle East'
        WHEN Country IN ('Armenia', 'Azerbaijan', 'Georgia') THEN 'Caucaus'
        ELSE 'Other'
    END AS region,
    SUM(Population) AS total_population,
    MAX(Country) AS largest_country,
    MAX(Population) AS largest_population,
    MIN(Country) AS smallest_country,
    MIN(Population) AS smallest_population
FROM List
GROUP BY region
ORDER BY region   
""")
    db.print_data()
    db.close()






