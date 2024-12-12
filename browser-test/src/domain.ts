type Conn = {
  query: (query: string) => Promise<any>;
  exec: (query: string) => Promise<any>;
};

export function processInput(
  input: string,
  playerId: number,
  backend: boolean
): string[] {
  if (input.length === 0) {
    return [];
  }

  const queries = [];

  queries.push(
    `INSERT INTO input(input, player_id, event_id) (SELECT '${input}' as input, ${playerId} as player_id, MAX(event_id) + 1 as event_id FROM input LIMIT 1);`
  );

  for (const update of input.split("|")) {
    const [key, val] = update.split(":");

    queries.push(
      `INSERT INTO mutable(key, val, player_id, event_id) (SELECT '${key}' as key, '${val}' as val, ${playerId} as player_id, MAX(event_id) as event_id FROM input LIMIT 1);`
    );

    if (backend) {
      queries.push(
        `DELETE FROM mutable WHERE player_id = ${playerId} AND key = '${key}' AND event_id <> (SELECT MAX(event_id) FROM mutable WHERE key = '${key}' AND player_id = ${playerId});`
      );
    }
  }

  return queries;
}

export function setupSchema(): string {
  return `
  CREATE TABLE input(
        input VARCHAR(255) NOT NULL,
        player_id INTEGER NOT NULL,
        event_id INTEGER NOT NULL,
        PRIMARY KEY(player_id, event_id)
      );
      
  CREATE TABLE mutable(
        key TEXT NOT NULL,
        val INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        event_id INTEGER NOT NULL,
        PRIMARY KEY(player_id, event_id, key)
      );
  `;
}

export function initialSetup(): string {
  return `
BEGIN;

INSERT INTO input (input, player_id, event_id) VALUES
  ('health:100|xp:0|mana:100', 0, 0),
  ('health:100|xp:0|mana:100', 1, 0);

INSERT INTO mutable (key, val, player_id, event_id) VALUES
  ('health', 100, 0, 0),
  ('mana', 100, 0, 0),
  ('xp', 0, 0, 0);

INSERT INTO mutable (key, val, player_id, event_id) VALUES
  ('health', 100, 1, 0),
  ('mana', 100, 1, 0),
  ('xp', 0, 1, 0);

COMMIT;
`;
}
