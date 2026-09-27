// Generates and replays checkpoint fixtures using the pinned upstream Go SDK.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/earendil-works/absurd/sdks/go/absurd"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// main writes fixtures or verifies replay from an optional fixture argument.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run executes the fixture workflow in a fresh database supplied by pgdb.
func run() error {
	ctx := context.Background()
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer db.Close()
	schema, err := os.ReadFile("testdata/absurd.sql")
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, string(schema)); err != nil {
		return err
	}
	client, err := absurd.New(absurd.Options{DB: db, QueueName: "default"})
	if err != nil {
		return err
	}
	if err := client.CreateQueue(ctx, "default"); err != nil {
		return err
	}
	replay := len(os.Args) > 1
	task := absurd.Task("fixture", func(ctx context.Context, _ any) (any, error) {
		for i := 1; i <= 3; i++ {
			value, err := absurd.Step(ctx, "charge", func(context.Context) (int, error) {
				if replay {
					return 0, fmt.Errorf("checkpoint missed")
				}
				return i * 7, nil
			})
			if err != nil {
				return nil, err
			}
			if value != i*7 {
				return nil, fmt.Errorf("wrong step occurrence: %d", value)
			}
		}
		for _, year := range []int{2000, 2001} {
			wake := time.Date(year, 1, 1, 0, 0, 0, 123456789, time.UTC)
			if replay {
				wake = time.Now().Add(time.Hour)
			}
			if err := absurd.SleepUntil(ctx, "nap", wake); err != nil {
				return nil, err
			}
		}
		for i := 0; i < 2; i++ {
			value, err := absurd.AwaitEvent[int](ctx, "ready")
			if err != nil {
				return nil, err
			}
			if value != 99 {
				return nil, fmt.Errorf("wrong event payload: %d", value)
			}
		}
		for name, snapshot := range map[string]absurd.TaskResultSnapshot{
			"child-completed": {State: absurd.TaskCompleted, Result: json.RawMessage(`{"value":42}`)},
			"child-null":      {State: absurd.TaskCompleted, Result: json.RawMessage(`null`)},
			"child-failed":    {State: absurd.TaskFailed, Failure: json.RawMessage(`{"message":"failed"}`)},
			"child-cancelled": {State: absurd.TaskCancelled},
		} {
			if _, err := absurd.Step(ctx, name, func(context.Context) (absurd.TaskResultSnapshot, error) {
				if replay {
					return absurd.TaskResultSnapshot{}, fmt.Errorf("child snapshot missed")
				}
				return snapshot, nil
			}); err != nil {
				return nil, err
			}
		}
		return "done", nil
	})
	if err := client.Register(task); err != nil {
		return err
	}
	spawned, err := task.Spawn(ctx, client, nil)
	if err != nil {
		return err
	}
	if replay {
		data, err := os.ReadFile(os.Args[1])
		if err != nil {
			return err
		}
		var checkpoints map[string]json.RawMessage
		if err := json.Unmarshal(data, &checkpoints); err != nil {
			return err
		}
		for name, state := range checkpoints {
			if _, err := db.ExecContext(ctx, `SELECT absurd.set_task_checkpoint_state('default', $1, $2, $3, $4, null)`, spawned.TaskID, name, string(state), spawned.RunID); err != nil {
				return err
			}
		}
	} else {
		if _, err := db.ExecContext(ctx, `SELECT absurd.emit_event('default', 'ready', '99'::jsonb)`); err != nil {
			return err
		}
	}
	if err := client.WorkBatch(ctx); err != nil {
		return err
	}
	result, err := client.FetchTaskResult(ctx, "default", spawned.TaskID)
	if err != nil {
		return err
	}
	if result == nil || result.State != absurd.TaskCompleted {
		return fmt.Errorf("workflow did not complete: %+v", result)
	}
	rows, err := db.QueryContext(ctx, `SELECT checkpoint_name, state FROM absurd.get_task_checkpoint_states('default', $1, $2)`, spawned.TaskID, spawned.RunID)
	if err != nil {
		return err
	}
	defer rows.Close()
	checkpoints := make(map[string]json.RawMessage)
	for rows.Next() {
		var name string
		var state json.RawMessage
		if err := rows.Scan(&name, &state); err != nil {
			return err
		}
		checkpoints[name] = state
	}
	if err := rows.Err(); err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(checkpoints)
}
