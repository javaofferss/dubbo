/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.natives.thrift5;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.compiler.support.JavassistCompiler;

import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.function.BiFunction;

import org.codehaus.janino.SimpleCompiler;

import static org.apache.dubbo.rpc.protocol.natives.thrift5.Thrift5Protocol.THRIFT_ASYNC_IFACE;
import static org.apache.dubbo.rpc.protocol.natives.thrift5.Thrift5Protocol.THRIFT_IFACE;

/**
 * 直接代理.
 */
public class ThriftPoolDirectProxy {
    static JavassistCompiler compiler = new JavassistCompiler();

    static String PACKAGE = "package %s; \n";
    static String DEFAULT_IMPORT =
            "import org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool;\n"
                    + "import org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool.EndPoint;\n"
                    + "import org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftInterceptor;\n";

    static String IMPORT = "import %s; \n";
    static String CLASS = "public class ThriftPoolDirectProxy$%s implements %s { \n";
    static String PRO1 = "org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool pool;\n";
    static String PRO2 = "org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool.EndPoint key;\n";
    static String PRO3 = "ThriftInterceptor thriftInterceptor = new ThriftInterceptor();\n";

    static String C =
            "\npublic ThriftPoolDirectProxy$%s(org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool pool, org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool.EndPoint key) { "
                    + "        this.pool = pool;"
                    + "        this.key = key;"
                    + "    }";

    static String METHOD = "\npublic %s %s( %s )";
    static String METHOD_E = " {";
    static String METHOD_I = "Object o = null; Exception errObj = null;" + "        try {\n"
            + "            o = thriftInterceptor.borrowClient(pool, key);\n"
            + "            thriftInterceptor.before(o, \"%s\", %s);\n";
    static String METHOD_I_RETURN =
            "  %s res = ((%s)o).%s( %s ); thriftInterceptor.success(o, \"%s\", res); return res; \n";
    static String METHOD_I_VOID = "  ((%s)o).%s( %s ); thriftInterceptor.success(o, \"%s\", null); return ; \n";
    static String METHOD_I3 = " } catch (Exception e) {\n" + "            e.printStackTrace();\n"
            + "            thriftInterceptor.error(o, \"%s\", e); errObj = e; \n"
            + "            throw new RuntimeException(e);\n"
            + "        } finally {\n"
            + "            thriftInterceptor.over(o, \"%s\");\n"
            + "            thriftInterceptor.recycleClient(pool, key, o, errObj);\n"
            + "        }";
    static String METHOD_END = "}";

    static String CLASS_END = "}";

    public static Class directProxy(Class type) throws Throwable {
        String typeName = type.getName();
        String packageName = type.getPackage().getName();
        int idx = Math.max(typeName.indexOf(THRIFT_ASYNC_IFACE), typeName.indexOf(THRIFT_IFACE));
        String serviceClassName = typeName.substring(0, idx);
        String simpleClassName = serviceClassName.substring(serviceClassName.lastIndexOf(".") + 1);
        String asyncIfaceClassName = serviceClassName + THRIFT_ASYNC_IFACE;
        Class<?> asyncIfaceClass = Class.forName(asyncIfaceClassName);
        String syncIfaceClassName = serviceClassName + THRIFT_IFACE;
        Class<?> syncIfaceClass = Class.forName(syncIfaceClassName);

        String syncIfaceClassNameCode = syncIfaceClassName.replace("$", ".");
        String asyncIfaceClassNameCode = asyncIfaceClassName.replace("$", ".");
        // 生产代码.
        StringBuilder code = new StringBuilder();
        code.append(String.format(PACKAGE, packageName));
        code.append(DEFAULT_IMPORT);
        code.append(String.format(IMPORT, syncIfaceClassNameCode));
        code.append(String.format(IMPORT, asyncIfaceClassNameCode));
        code.append(String.format(CLASS, simpleClassName, syncIfaceClassNameCode + "," + asyncIfaceClassNameCode));
        //        code.append(String.format(LOG,simpleClassName));
        code.append(PRO1);
        code.append(PRO2);
        code.append(PRO3);

        code.append(String.format(C, simpleClassName));

        // 实现抽象方法
        Method[] declaredMethods = syncIfaceClass.getDeclaredMethods();
        processMethods(declaredMethods, code, syncIfaceClassNameCode);
        Method[] declaredMethods2 = asyncIfaceClass.getDeclaredMethods();
        processMethods(declaredMethods2, code, asyncIfaceClassNameCode);
        code.append(CLASS_END);
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(type.getClassLoader());

        // 编译源代码
        compiler.cook(new StringReader(code.toString()));
        // 加载编译后的类
        Class<?> proxyClass =
                compiler.getClassLoader().loadClass(packageName + ".ThriftPoolDirectProxy$" + simpleClassName);
        return proxyClass;
    }

    private static void processMethods(Method[] declaredMethods, StringBuilder code, String faceClassNameCode) {
        for (Method method : declaredMethods) {
            if (Modifier.isAbstract(method.getModifiers())) {
                // 获取返回类型;
                Class<?> returnType = method.getReturnType();
                String methodName = method.getName();
                // 获取参数类型
                Class<?>[] parameterTypes = method.getParameterTypes();
                StringBuilder pcs = new StringBuilder();
                StringBuilder pcObjs = new StringBuilder();
                String arg = "arg";
                int position = 0;
                for (Class pc : parameterTypes) {
                    if (position != 0) {
                        pcs.append(",");
                        pcObjs.append(",");
                    }
                    pcs.append(pc.getName());
                    pcs.append(" ");
                    pcs.append(arg);
                    pcs.append(position);

                    pcObjs.append(arg);
                    pcObjs.append(position);

                    position++;
                }
                // 拼装代码
                code.append(String.format(METHOD, returnType.getName(), methodName, pcs));
                code.append(METHOD_E);
                code.append(String.format(METHOD_I, methodName, pcObjs));
                if (returnType == Void.class || returnType == void.class) {
                    code.append(String.format(METHOD_I_VOID, faceClassNameCode, methodName, pcObjs, methodName));
                } else {
                    code.append(String.format(
                            METHOD_I_RETURN, returnType.getName(), faceClassNameCode, methodName, pcObjs, methodName));
                }
                code.append(String.format(METHOD_I3, methodName, methodName));
                code.append(METHOD_END);
            }
        }
    }

    public static <T> T getProxyObject(Class clazz, URL url, BiFunction<Class, URL, Object> processRefer)
            throws Exception {
        try {
            Class proxyClass = directProxy(clazz);
            Constructor declaredConstructors = proxyClass.getDeclaredConstructor(
                    ThriftGenericKeyedObjectPool.class, ThriftGenericKeyedObjectPool.EndPoint.class);
            ThriftGenericKeyedObjectPool instance = ThriftGenericKeyedObjectPool.getInstance(processRefer);
            ThriftGenericKeyedObjectPool.EndPoint endPoint = new ThriftGenericKeyedObjectPool.EndPoint(clazz, url);
            Object o = declaredConstructors.newInstance(instance, endPoint);
            return (T) o;
        } catch (Throwable throwable) {
            // 这里可以退化成动态代理
            throwable.printStackTrace();
        }
        return ThriftPoolProxy.getProxyObject(clazz, url, processRefer);
    }
}
