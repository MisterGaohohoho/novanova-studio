import { createTranslator, type AbstractIntlMessages, type TranslationValues } from "next-intl";
import { websiteMessages } from "./messages";
import { isWebsiteLocale, languageTag, type WebsiteLocale } from "./locale";

/** 读取客户端当前语言，服务端调用请显式传入请求语言。 */
export function currentWebsiteLocale(): WebsiteLocale {
    const locale = typeof document === "undefined" ? undefined : document.documentElement.dataset.language;
    return isWebsiteLocale(locale) ? locale : "zh-cn";
}

/** 为非组件的客户端业务动作创建翻译器，不缓存翻译后的业务数据。 */
const translators = {
    "zh-cn": createTranslator({ locale: "zh-CN", messages: websiteMessages["zh-cn"] as AbstractIntlMessages }),
    en: createTranslator({ locale: "en", messages: websiteMessages.en as AbstractIntlMessages }),
};

/** 按当前语言翻译固定系统文案。 */
export function translateMessage(key: string, values?: TranslationValues, locale = currentWebsiteLocale()): string {
    return translators[locale](key, values);
}

/** 日期和数字格式化共用语言标签，不改变时区。 */
export function currentLanguageTag(): string {
    return languageTag(currentWebsiteLocale());
}
