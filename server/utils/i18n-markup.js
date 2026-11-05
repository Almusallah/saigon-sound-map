/* The page's keyed words, written into its markup in one language:
   <tag data-i18n="k">text</tag>, <tag data-i18n-html="k">markup</tag>,
   data-i18n-attr="attr:k;attr:k", and the meta/og description. The
   Vietnamese copy lives in client/index.html itself (scripts/i18n-html.cjs
   writes it); the server makes the English one from it at boot, so an
   English link paints English before any script has loaded. */
const escText = s => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const escAttr = s => escText(s).replace(/"/g, '&quot;');

// Returns the rewritten html and the keys whose words were out of step.
function sync(html, t) {
  const stale = [];
  const meta = escAttr(t('meta.description'));
  html = html.replace(/(<meta (?:name="description"|property="og:description") content=")([^"]*)/g, (m, p1, old) => {
    if (old !== meta) stale.push('meta.description');
    return p1 + meta;
  });
  html = html.replace(/<(\w+)((?:[^>"]|"[^"]*")*?\sdata-i18n(-html)?="([^"]+)"(?:[^>"]|"[^"]*")*)>([\s\S]*?)<\/\1>/g,
    (m, tag, attrs, isHtml, key, inner) => {
      const want = isHtml ? t(key) : escText(t(key));
      if (inner !== want) stale.push(key);
      return `<${tag}${attrs}>${want}</${tag}>`;
    });
  html = html.replace(/<(\w+)((?:[^>"]|"[^"]*")*?\sdata-i18n-attr="([^"]+)"(?:[^>"]|"[^"]*")*)>/g, (m, tag, attrs, spec) => {
    for (const pair of spec.split(';')) {
      const [attr, key] = pair.split(':');
      const want = escAttr(t(key));
      const re = new RegExp(`(\\s${attr}=")([^"]*)(")`);
      if (re.test(attrs)) {
        const old = attrs.match(re)[2];
        if (old !== want) stale.push(`${key} (${attr})`);
        attrs = attrs.replace(re, (_, a, __, c) => a + want + c);
      } else {
        stale.push(`${key} (${attr} missing)`);
        attrs += ` ${attr}="${want}"`;
      }
    }
    return `<${tag}${attrs}>`;
  });
  return { html, stale };
}

// The install manifest describes the app in the default language.
function syncManifest(text, t, lang) {
  const want = JSON.stringify(t('meta.description'));
  const stale = [];
  text = text.replace(/("description": )("(?:[^"\\]|\\.)*")/, (m, p1, old) => {
    if (old !== want) stale.push('manifest description');
    return p1 + want;
  });
  if (!/"lang": /.test(text)) {
    stale.push('manifest lang');
    text = text.replace(/(\n(\s*)"description": [^\n]*\n)/, `$1$2"lang": "${lang}",\n`);
  }
  return { text, stale };
}

module.exports = { sync, syncManifest };
