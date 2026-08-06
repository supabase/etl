import { absoluteUrl } from '@/lib/site';
import { MarkdownCopyButton } from 'fumadocs-ui/layouts/docs/page';
import type { SVGProps } from 'react';

function OpenAIIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" {...props}>
      <path d="M22.28 9.82a5.98 5.98 0 0 0-.52-4.91 6.05 6.05 0 0 0-6.51-2.9A6.07 6.07 0 0 0 4.98 4.18a5.98 5.98 0 0 0-4 2.9 6.05 6.05 0 0 0 .75 7.1 5.98 5.98 0 0 0 .51 4.91 6.05 6.05 0 0 0 6.51 2.9A5.98 5.98 0 0 0 13.26 24a6.06 6.06 0 0 0 5.77-4.21 5.99 5.99 0 0 0 4-2.9 6.06 6.06 0 0 0-.75-7.07Zm-9.02 12.61a4.48 4.48 0 0 1-2.88-1.04l.14-.08 4.78-2.76a.79.79 0 0 0 .4-.68v-6.74l2.02 1.17a.07.07 0 0 1 .04.05v5.58a4.5 4.5 0 0 1-4.5 4.5Zm-9.66-4.13a4.47 4.47 0 0 1-.54-3.01l.14.08 4.79 2.76a.77.77 0 0 0 .78 0l5.84-3.37v2.33a.08.08 0 0 1-.03.07l-4.84 2.79A4.5 4.5 0 0 1 3.6 18.3ZM2.34 7.9A4.49 4.49 0 0 1 4.7 5.92v5.68a.77.77 0 0 0 .39.68l5.81 3.35-2.02 1.17a.08.08 0 0 1-.07 0l-4.83-2.79A4.5 4.5 0 0 1 2.34 7.9Zm16.6 3.85-5.84-3.39 2.02-1.16a.08.08 0 0 1 .07 0l4.83 2.79a4.49 4.49 0 0 1-.68 8.1v-5.67a.79.79 0 0 0-.4-.67Zm2.01-3.02-.14-.09-4.78-2.78a.78.78 0 0 0-.78 0L9.41 9.23V6.9a.07.07 0 0 1 .03-.06l4.83-2.79a4.5 4.5 0 0 1 6.68 4.68ZM8.31 12.86 6.29 11.7a.08.08 0 0 1-.04-.06V6.07a4.5 4.5 0 0 1 7.38-3.45l-.15.08L8.7 5.46a.79.79 0 0 0-.39.68Zm1.1-2.36 2.6-1.5 2.6 1.5v3l-2.6 1.5-2.6-1.5Z" />
    </svg>
  );
}

function AnthropicIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" {...props}>
      <path d="M17.3 3.54h-3.67l6.7 16.92H24Zm-10.6 0L0 20.46h3.74l1.37-3.55h7l1.38 3.55h3.74l-6.7-16.92Zm-.38 10.22 2.3-5.94 2.29 5.94Z" />
    </svg>
  );
}

export function AgentActions({ markdownUrl, pagePath }: { markdownUrl: string; pagePath: string }) {
  const pageUrl = absoluteUrl(pagePath);
  const prompt = `Read ${pageUrl}, I want to ask questions about it.`;

  return (
    <div className="etl-agent-actions" aria-label="Agent-readable documentation">
      <MarkdownCopyButton markdownUrl={markdownUrl} />
      <a
        href={`https://chatgpt.com/?${new URLSearchParams({ hints: 'search', q: prompt })}`}
        target="_blank"
        rel="noreferrer noopener"
      >
        <OpenAIIcon />
        Ask ChatGPT
      </a>
      <a
        href={`https://claude.ai/new?${new URLSearchParams({ q: prompt })}`}
        target="_blank"
        rel="noreferrer noopener"
      >
        <AnthropicIcon />
        Ask Claude
      </a>
    </div>
  );
}
