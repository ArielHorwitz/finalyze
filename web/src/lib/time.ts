/** How long ago a timestamp was, e.g. "just now" or "2 h ago". */
export function ago(timestamp: string, now = Date.now()): string {
  const minutes = Math.round((now - new Date(timestamp).getTime()) / 60000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours} h ago`;
  return `${Math.round(hours / 24)} days ago`;
}

