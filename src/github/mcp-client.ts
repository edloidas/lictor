export {};

const [socketPath, jobId, encodedWork] = process.argv.slice(2);
if (socketPath === undefined || jobId === undefined || encodedWork === undefined) process.exit(2);

const forward = (request: unknown) =>
  new Promise<string>((resolve, reject) => {
    let output = '';
    Bun.connect({
      unix: socketPath,
      socket: {
        open(socket) {
          socket.write(
            `${JSON.stringify({ command: 'capability.mcp', args: [jobId, encodedWork, JSON.stringify(request)] })}\n`,
          );
        },
        data(_socket, data) {
          output += Buffer.from(data).toString('utf8');
        },
        close() {
          resolve(output);
        },
        error(_socket, error) {
          reject(error);
        },
      },
    }).catch(reject);
  });

let input = '';
for await (const chunk of Bun.stdin.stream()) {
  input += Buffer.from(chunk).toString('utf8');
  let newline = input.indexOf('\n');
  while (newline >= 0) {
    const line = input.slice(0, newline);
    input = input.slice(newline + 1);
    if (line.trim() !== '') {
      const envelope = JSON.parse(await forward(JSON.parse(line))) as {
        ok: boolean;
        result?: unknown;
        error?: unknown;
      };
      process.stdout.write(
        `${JSON.stringify(envelope.ok ? envelope.result : { jsonrpc: '2.0', id: null, error: envelope.error })}\n`,
      );
    }
    newline = input.indexOf('\n');
  }
}
