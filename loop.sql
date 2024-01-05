DO $$
	DECLARE
		counter INT := 1;
	BEGIN
		WHILE counter < 5 LOOP
			INSERT INTO actors (actor_id, actor_name, gender)
			VALUES (30000000 + counter, 'Actor ' || counter,
				   	CASE
				   	   WHEN counter % 2 = 1 THEN 1
				       ELSE 2
				   	END);
			counter := counter + 1;
		END LOOP;
END $$;
