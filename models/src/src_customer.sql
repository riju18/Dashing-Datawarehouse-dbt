WITH customer_info AS (
    SELECT
        cc.customer_id,
        cc.first_name,
        cc.last_name,
        cc.email,
        cc.create_date,
        cc.last_update,
            CASE
                WHEN cc.active = 1 THEN 'Active'::text
                ELSE 'Inactive'::text
            END AS customer_is_active,
        aa.address_id,
        aa.address,
        aa.address2,
        ct.city_id,
        ct.city,
        aa.district,
        cy.country_id,
        cy.country,
        aa.postal_code,
        aa.phone
       FROM customer cc
         JOIN address aa ON aa.address_id = cc.address_id
         JOIN city ct ON ct.city_id = aa.city_id
         JOIN country cy ON cy.country_id = ct.country_id
      ORDER BY cc.customer_id
    )
SELECT * FROM customer_info