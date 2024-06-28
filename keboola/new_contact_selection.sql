-- Skript vytváří nebo nahrazuje tabulku "db_pomoci_verified_contacts" s ověřenými kontakty.
-- Skládá se ze dvou hlavních částí: počítání emailů a telefonů a filtrování kontaktů.

-- Část 1: Počítání emailů a telefonů
-- V této části se počítá počet emailů a telefonů pro každý kontakt.
WITH email_and_phone_count AS (
    SELECT
        *,
        SUM(CASE WHEN REGEXP_LIKE(TRIM("New_Contact"), '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}') THEN 1 ELSE 0 END) AS Email_Count, -- Počítá počet emailů
        SUM(CASE WHEN REGEXP_LIKE(TRIM("New_Contact"), '^[+0-9].*') THEN 1 ELSE 0 END) AS Phone_Count -- Počítá počet telefonů
    FROM 
        "out.c-mapa-pomoci-output"."db_pomoci_flagged"
    GROUP BY 
        "Nazev", "Kategorie", "Adresa", "Webova_stranka", "E_mail", "Telefon", "Matched", "New_Contact", "New_Matched", "Source","_timestamp"
)

-- Část 2: Filtrování kontaktů
-- V této části se vytváří seznamy nových emailů a telefonů pro každý kontakt.
, filtered_contacts AS (
    SELECT
        *,
        ARRAY_AGG(CASE WHEN REGEXP_LIKE(TRIM("New_Contact"), '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}') THEN TRIM("New_Contact") END) WITHIN GROUP (ORDER BY "New_Contact") AS New_Emails, -- Seskupuje nové emaily
        ARRAY_AGG(CASE WHEN REGEXP_LIKE(TRIM("New_Contact"), '^[+0-9].*') THEN TRIM("New_Contact") END) WITHIN GROUP (ORDER BY "New_Contact") AS New_Phones -- Seskupuje nové telefony
    FROM email_and_phone_count
    GROUP BY 
        "Nazev", "Kategorie", "Adresa", "Webova_stranka", "E_mail", "Telefon", "Matched", "New_Contact", "New_Matched", "Source","_timestamp", Email_Count, Phone_Count
)

-- Finální výběr
-- Zde se vybírají a upravují konečné hodnoty emailů, telefonů a dalších atributů pro tabulku "db_pomoci_verified_contacts".
SELECT 
    "Nazev",
    "Kategorie",
    "Adresa",
    "Webova_stranka",
    CASE
        WHEN "Matched" = 'new_email_match' THEN COALESCE(New_Emails[0], "E_mail") -- Pokud je nový emailový kontakt, použije se nový email
        WHEN "Matched" = 'new_contact_both_match_with_email' AND Email_Count = 1 THEN COALESCE(New_Emails[0], "E_mail") -- Pokud je nový kontakt a je pouze jeden email, použije se nový email
        ELSE "E_mail" -- Jinak se použije původní email
    END AS "E_mail",
    CASE
        WHEN "Matched" = 'new_phone_match' THEN COALESCE(New_Phones[0], "Telefon") -- Pokud je nový telefonní kontakt, použije se nový telefon
        WHEN "Matched" = 'new_contact_both_match_with_email' AND Phone_Count = 1 THEN COALESCE(New_Phones[0], "Telefon") -- Pokud je nový kontakt a je pouze jeden telefon, použije se nový telefon
        ELSE "Telefon" -- Jinak se použije původní telefon
    END AS "Telefon",
    CASE
        WHEN "Matched" IN ('new_email_match', 'new_contact_both_match_with_email') AND Email_Count > 1 THEN 'Email' -- Pokud je více než jeden email, je třeba manuální kontrola
        WHEN "Matched" IN ('new_phone_match', 'new_contact_both_match_with_email') AND Phone_Count > 1 THEN 'Phone' -- Pokud je více než jeden telefon, je třeba manuální kontrola
        ELSE 'No' -- Jinak není potřeba manuální kontrola
    END AS "Manual_Check_Needed",
    FALSE AS "Skip_automatic_verification", -- Automatická verifikace nebude přeskočena
    CURRENT_TIMESTAMP AS "Last_checked" -- Datum a čas poslední kontroly
FROM 
    filtered_contacts
;
