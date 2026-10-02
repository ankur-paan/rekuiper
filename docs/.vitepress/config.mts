import { defineConfig } from 'vitepress';
import { withMermaid } from 'vitepress-plugin-mermaid';
import directory from '../directory.json';

interface DirectoryItem {
  title: string;
  path?: string;
  collapsed?: boolean;
  children?: DirectoryItem[];
}

function transformSidebar(items: DirectoryItem[]): any[] {
  return items.map((item) => {
    const entry: any = { text: item.title };
    if (item.path) {
      entry.link = item.path === '.' || item.path === './'
        ? '/'
        : item.path.startsWith('/')
          ? item.path
          : `/${item.path}`;
    }
    if (item.collapsed !== undefined) {
      entry.collapsed = item.collapsed;
    }
    if (item.children && item.children.length > 0) {
      entry.items = transformSidebar(item.children);
    }
    return entry;
  });
}

export default withMermaid(
  defineConfig({
  title: 'rekuiper',
  description: 'Lightweight Stream Processing Engine in Rust',
  srcDir: './en_US',
  outDir: './.vitepress/dist',
  ignoreDeadLinks: true,
  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/rules/overview' },
      { text: 'Benchmarks', link: '/benchmarks/overview' },
      { text: 'SQL Reference', link: '/sqls/overview' },
      { text: 'API Reference', link: '/api/restapi/overview' },
      { text: 'GitHub', link: 'https://github.com/ankur-paan/rekuiper' }
    ],
    sidebar: transformSidebar((directory as any).en || []),
    socialLinks: [
      { icon: 'github', link: 'https://github.com/ankur-paan/rekuiper' }
    ],
    search: {
      provider: 'local'
    },
    footer: {
      message: 'Released under the Apache-2.0 / MIT License.',
      copyright: 'Copyright © 2026 rekuiper contributors'
    }
  }
}));
