import { PipelinesMark } from '@/components/brand';
import { Mermaid } from '@/components/mermaid';
import defaultMdxComponents from 'fumadocs-ui/mdx';
import { Blocks, BookOpenText, Database, RefreshCw, Rocket, type LucideIcon } from 'lucide-react';
import type { MDXComponents } from 'mdx/types';

const docPathIcons: Record<string, LucideIcon> = {
  Blocks,
  BookOpenText,
  Database,
  RefreshCw,
  Rocket,
};

function DocPathIcon({ name }: { name: string }) {
  if (name === 'Pipelines') return <PipelinesMark className="doc-path-pipelines-icon" />;

  const Icon = docPathIcons[name];
  return Icon ? <Icon aria-hidden="true" /> : null;
}

export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
    DocPathIcon,
    Mermaid,
    PipelinesMark,
    ...components,
  } satisfies MDXComponents;
}

export const useMDXComponents = getMDXComponents;

declare global {
  type MDXProvidedComponents = ReturnType<typeof getMDXComponents>;
}
