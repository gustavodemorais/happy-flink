# happy-flink

We all know Flink is super powerful, but it can be hard to make the squirrel happy. This repo is a collection of rules for spotting Flink SQL and Table API problems and their fixes.

## What

The goal of the repo is to help people fix but also learn some of Flink and stream processing concepts. The idea is for you to point your AI to the INDEX.md file and it'll guide you through the rules.

Rules cover the usual pain: slow jobs, state that only grows, state that never shrinks, backpressure.

## Who it's for

You run Flink SQL or Table API and want to know why a job is slow or why its state keeps growing, and you want a fix.

## How to use it

1. Open your favorite AI and point it to the [INDEX.md](INDEX.md) file.
2. Provide the AI with the data you have on hand, ideally all three:
   1. only SQL text -> `sql/`
   2. an EXPLAIN or compiled plan -> `plan/`
   3. a live job with per-node metrics -> `runtime/`
3. The AI will scan the rules and provide you with a list of possible problems and fixes and is ready to explain the reasoning behind each!

Cheaper inputs catch less; richer inputs catch more.

## License

MIT. See [LICENSE](LICENSE).
