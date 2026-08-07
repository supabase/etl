import { PipelinesMark } from '@/components/brand';
import { Mermaid } from '@/components/mermaid';
import defaultMdxComponents from 'fumadocs-ui/mdx';
import {
  ArchiveX,
  BadgeCheck,
  Blocks,
  BookOpenText,
  Construction,
  Database,
  DatabaseZap,
  RefreshCw,
  Rocket,
  Terminal,
  type LucideIcon,
} from 'lucide-react';
import type { MDXComponents } from 'mdx/types';

const docPathIcons: Record<string, LucideIcon> = {
  Blocks,
  BookOpenText,
  Database,
  DatabaseZap,
  RefreshCw,
  Rocket,
  Terminal,
};

const destinationStatuses = {
  stable: { icon: BadgeCheck, label: 'Stable' },
  'in-progress': { icon: Construction, label: 'In progress' },
  deprecated: { icon: ArchiveX, label: 'Deprecated' },
} as const;

function DocPathIcon({ name }: { name: string }) {
  if (name === 'Pipelines') return <PipelinesMark className="doc-path-pipelines-icon" />;

  const Icon = docPathIcons[name];
  return Icon ? <Icon aria-hidden="true" strokeWidth={1.5} /> : null;
}

function DestinationStatus({ status }: { status: keyof typeof destinationStatuses }) {
  const { icon: Icon, label } = destinationStatuses[status];

  return (
    <span className="etl-destination-status" data-status={status}>
      <Icon aria-hidden="true" strokeWidth={1.8} />
      {label}
    </span>
  );
}

export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
    DestinationStatus,
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
