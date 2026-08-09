'use client';

import { useTheme } from 'next-themes';
import { useEffect, useId, useState } from 'react';

export function Mermaid({ chart }: { chart: string }) {
  const id = useId().replaceAll(':', '');
  const { resolvedTheme } = useTheme();
  const [svg, setSvg] = useState<string>();
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let active = true;

    void import('mermaid')
      .then(async ({ default: mermaid }) => {
        mermaid.initialize({
          startOnLoad: false,
          securityLevel: 'strict',
          fontFamily: 'Inter Variable, ui-sans-serif, system-ui, sans-serif',
          theme: resolvedTheme === 'dark' ? 'dark' : 'neutral',
          themeVariables: {
            primaryColor: resolvedTheme === 'dark' ? '#123227' : '#e7f8ef',
            primaryTextColor: resolvedTheme === 'dark' ? '#f3f7f5' : '#15211c',
            primaryBorderColor: '#3ecf8e',
            lineColor: resolvedTheme === 'dark' ? '#7e968b' : '#557166',
            fontSize: '15px',
          },
        });

        const result = await mermaid.render(`etl-mermaid-${id}`, chart);
        if (active) {
          setFailed(false);
          setSvg(result.svg);
        }
      })
      .catch(() => {
        if (active) setFailed(true);
      });

    return () => {
      active = false;
    };
  }, [chart, id, resolvedTheme]);

  if (failed) {
    return (
      <pre className="mermaid-fallback" aria-label="Diagram source">
        <code>{chart}</code>
      </pre>
    );
  }

  return (
    <div className="mermaid-diagram" aria-busy={!svg}>
      {svg ? <div dangerouslySetInnerHTML={{ __html: svg }} /> : <div className="mermaid-skeleton" />}
    </div>
  );
}
