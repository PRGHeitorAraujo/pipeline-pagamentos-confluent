-- Dados sintéticos: 50 contas. As transações são geradas ao vivo por app/generator.py.
-- Cada conta tem 2 cartões: card-<account_id com 4 dígitos>-1 e -2.
INSERT INTO accounts (holder_name, segment, home_city, risk_tier, credit_limit)
SELECT
    (ARRAY['Ana','Bruno','Carla','Diego','Elisa','Felipe','Gabriela','Heitor','Isabela','João'])[1 + (g % 10)]
      || ' ' ||
    (ARRAY['Silva','Souza','Lima','Costa','Araújo','Melo','Barbosa','Rocha','Dias','Nunes'])[1 + ((g * 7) % 10)],
    (ARRAY['standard','premium','business'])[1 + (g % 3)],
    (ARRAY['Recife','São Paulo','Rio de Janeiro','Belo Horizonte','Salvador','Fortaleza','Curitiba','Porto Alegre'])[1 + (g % 8)],
    (ARRAY['low','low','medium','high'])[1 + (g % 4)],
    (1000 + (g % 10) * 1500)::NUMERIC(12, 2)
FROM generate_series(1, 50) AS g
WHERE NOT EXISTS (SELECT 1 FROM accounts);
