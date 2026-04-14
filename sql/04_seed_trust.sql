INSERT INTO trust_state (id, state) VALUES (1, 'blocked')
ON CONFLICT (id) DO NOTHING;
