// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.ValueGeneration;

namespace Microsoft.EntityFrameworkCore.ModelBuilding
{
    public class ModelBuilderNonGenericTest : ModelBuilderTest
    {
        public class NonGenericNonRelationship : NonRelationshipTestBase
        {
            protected override TestModelBuilder CreateTestModelBuilder(TestHelpers testHelpers)
                => new NonGenericTestModelBuilder(testHelpers);
        }

        public class NonGenericInheritance : InheritanceTestBase
        {
            protected override TestModelBuilder CreateTestModelBuilder(TestHelpers testHelpers)
                => new NonGenericTestModelBuilder(testHelpers);
        }

        public class NonGenericOwnedTypes : OwnedTypesTestBase
        {
            protected override TestModelBuilder CreateTestModelBuilder(TestHelpers testHelpers)
                => new NonGenericTestModelBuilder(testHelpers);
        }

        public class NonGenericKeylessEntities : KeylessEntitiesTestBase
        {
            protected override TestModelBuilder CreateTestModelBuilder(TestHelpers testHelpers)
                => new NonGenericTestModelBuilder(testHelpers);
        }

        public class NonGenericOneToMany : OneToManyTestBase
        {
            protected override TestModelBuilder CreateTestModelBuilder(TestHelpers testHelpers)
                => new NonGenericTestModelBuilder(testHelpers);
        }

        public class NonGenericManyToOne : ManyToOneTestBase
        {
            protected override TestModelBuilder CreateTestModelBuilder(TestHelpers testHelpers)
                => new NonGenericTestModelBuilder(testHelpers);
        }

        public class NonGenericManyToMany : ManyToManyTestBase
        {
            protected override TestModelBuilder CreateTestModelBuilder(TestHelpers testHelpers)
                => new NonGenericTestModelBuilder(testHelpers);
        }

        public class NonGenericOneToOne : OneToOneTestBase
        {
            protected override TestModelBuilder CreateTestModelBuilder(TestHelpers testHelpers)
                => new NonGenericTestModelBuilder(testHelpers);
        }

        private class NonGenericTestModelBuilder : TestModelBuilder
        {
            public NonGenericTestModelBuilder(TestHelpers testHelpers)
                : base(testHelpers)
            {
            }

            public override TestEntityTypeBuilder<TEntity> Entity<TEntity>()
                => new NonGenericTestEntityTypeBuilder<TEntity>(ModelBuilder.Entity(typeof(TEntity)));

            public override TestEntityTypeBuilder<TEntity> SharedEntity<TEntity>(string name)
                => new NonGenericTestEntityTypeBuilder<TEntity>(ModelBuilder.SharedEntity(name, typeof(TEntity)));

            public override TestModelBuilder Entity<TEntity>(Action<TestEntityTypeBuilder<TEntity>> buildAction)
            {
                ModelBuilder.Entity(
                    typeof(TEntity), entityTypeBuilder =>
                        buildAction(new NonGenericTestEntityTypeBuilder<TEntity>(entityTypeBuilder)));
                return this;
            }

            public override TestModelBuilder SharedEntity<TEntity>(string name, Action<TestEntityTypeBuilder<TEntity>> buildAction)
            {
                ModelBuilder.SharedEntity(
                    name,
                    typeof(TEntity), entityTypeBuilder =>
                        buildAction(new NonGenericTestEntityTypeBuilder<TEntity>(entityTypeBuilder)));
                return this;
            }

            public override TestOwnedEntityTypeBuilder<TEntity> Owned<TEntity>()
                => new NonGenericTestOwnedEntityTypeBuilder<TEntity>(ModelBuilder.Owned(typeof(TEntity)));

            public override TestSharedEntityTypeBuilder<TEntity> SharedEntity<TEntity>()
                => new NonGenericTestSharedEntityTypeBuilder<TEntity>(ModelBuilder.SharedEntity(typeof(TEntity)));

            public override TestModelBuilder Ignore<TEntity>()
            {
                ModelBuilder.Ignore(typeof(TEntity));
                return this;
            }
        }

        protected class NonGenericTestEntityTypeBuilder<TEntity> : TestEntityTypeBuilder<TEntity>, IInfrastructure<EntityTypeBuilder>
            where TEntity : class
        {
            public NonGenericTestEntityTypeBuilder(EntityTypeBuilder entityTypeBuilder)
            {
                EntityTypeBuilder = entityTypeBuilder;
            }

            protected EntityTypeBuilder EntityTypeBuilder { get; }
            public override IMutableEntityType Metadata => EntityTypeBuilder.Metadata;

            protected virtual NonGenericTestEntityTypeBuilder<TEntity> Wrap(EntityTypeBuilder entityTypeBuilder)
                => new NonGenericTestEntityTypeBuilder<TEntity>(entityTypeBuilder);

            public override TestEntityTypeBuilder<TEntity> HasAnnotation(string annotation, object value)
                => Wrap(EntityTypeBuilder.HasAnnotation(annotation, value));

            public override TestEntityTypeBuilder<TEntity> HasBaseType<TBaseEntity>()
                => Wrap(EntityTypeBuilder.HasBaseType(typeof(TBaseEntity)));

            public override TestEntityTypeBuilder<TEntity> HasBaseType(string baseEntityTypeName)
                => Wrap(EntityTypeBuilder.HasBaseType(baseEntityTypeName));

            public override TestKeyBuilder<TEntity> HasKey(Expression<Func<TEntity, object>> keyExpression)
                => new NonGenericTestKeyBuilder<TEntity>(
                    EntityTypeBuilder.HasKey(keyExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestKeyBuilder<TEntity> HasKey(params string[] propertyNames)
                => new NonGenericTestKeyBuilder<TEntity>(EntityTypeBuilder.HasKey(propertyNames));

            public override TestKeyBuilder<TEntity> HasAlternateKey(Expression<Func<TEntity, object>> keyExpression)
                => new NonGenericTestKeyBuilder<TEntity>(
                    EntityTypeBuilder.HasAlternateKey(
                        keyExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestKeyBuilder<TEntity> HasAlternateKey(params string[] propertyNames)
                => new NonGenericTestKeyBuilder<TEntity>(EntityTypeBuilder.HasAlternateKey(propertyNames));

            public override TestEntityTypeBuilder<TEntity> HasNoKey()
                => Wrap(EntityTypeBuilder.HasNoKey());

            public override TestPropertyBuilder<TProperty> Property<TProperty>(Expression<Func<TEntity, TProperty>> propertyExpression)
            {
                var memberInfo = propertyExpression.GetMemberAccess();
                return new NonGenericTestPropertyBuilder<TProperty>(
                    EntityTypeBuilder.Property(memberInfo.GetMemberType(), memberInfo.GetSimpleMemberName()));
            }

            public override TestPropertyBuilder<TProperty> Property<TProperty>(string propertyName)
                => new NonGenericTestPropertyBuilder<TProperty>(EntityTypeBuilder.Property<TProperty>(propertyName));

            public override TestPropertyBuilder<TProperty> IndexerProperty<TProperty>(string propertyName)
                => new NonGenericTestPropertyBuilder<TProperty>(EntityTypeBuilder.IndexerProperty<TProperty>(propertyName));

            public override TestNavigationBuilder Navigation<TNavigation>(Expression<Func<TEntity, TNavigation>> navigationExpression)
                => new NonGenericTestNavigationBuilder(
                    EntityTypeBuilder.Navigation(navigationExpression.GetMemberAccess().GetSimpleMemberName()));

            public override TestNavigationBuilder Navigation(string propertyName)
                => new NonGenericTestNavigationBuilder(EntityTypeBuilder.Navigation(propertyName));

            public override TestEntityTypeBuilder<TEntity> Ignore(Expression<Func<TEntity, object>> propertyExpression)
                => Wrap(EntityTypeBuilder.Ignore(propertyExpression.GetMemberAccess().GetSimpleMemberName()));

            public override TestEntityTypeBuilder<TEntity> Ignore(string propertyName)
                => Wrap(EntityTypeBuilder.Ignore(propertyName));

            public override TestIndexBuilder<TEntity> HasIndex(Expression<Func<TEntity, object>> indexExpression)
                => new NonGenericTestIndexBuilder<TEntity>(
                    EntityTypeBuilder.HasIndex(indexExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestIndexBuilder<TEntity> HasIndex(params string[] propertyNames)
                => new NonGenericTestIndexBuilder<TEntity>(EntityTypeBuilder.HasIndex(propertyNames));

            public override TestOwnedNavigationBuilder<TEntity, TRelatedEntity> OwnsOne<TRelatedEntity>(string navigationName)
                => new NonGenericTestOwnedNavigationBuilder<TEntity, TRelatedEntity>(
                    EntityTypeBuilder.OwnsOne(typeof(TRelatedEntity), navigationName));
            public override TestEntityTypeBuilder<TEntity> OwnsOne<TRelatedEntity>(
                string navigationName,
                Action<TestOwnedNavigationBuilder<TEntity, TRelatedEntity>> buildAction)
                => Wrap(
                    EntityTypeBuilder.OwnsOne(
                        typeof(TRelatedEntity),
                        navigationName,
                        r => buildAction(new NonGenericTestOwnedNavigationBuilder<TEntity, TRelatedEntity>(r))));

            public override TestOwnedNavigationBuilder<TEntity, TRelatedEntity> OwnsOne<TRelatedEntity>(
                Expression<Func<TEntity, TRelatedEntity>> navigationExpression)
                => new NonGenericTestOwnedNavigationBuilder<TEntity, TRelatedEntity>(
                    EntityTypeBuilder.OwnsOne(typeof(TRelatedEntity), navigationExpression.GetMemberAccess().GetSimpleMemberName()));

            public override TestEntityTypeBuilder<TEntity> OwnsOne<TRelatedEntity>(
                Expression<Func<TEntity, TRelatedEntity>> navigationExpression,
                Action<TestOwnedNavigationBuilder<TEntity, TRelatedEntity>> buildAction)
                => Wrap(
                    EntityTypeBuilder.OwnsOne(
                        typeof(TRelatedEntity),
                        navigationExpression.GetMemberAccess().GetSimpleMemberName(),
                        r => buildAction(new NonGenericTestOwnedNavigationBuilder<TEntity, TRelatedEntity>(r))));

            public override TestOwnedNavigationBuilder<TEntity, TRelatedEntity> OwnsMany<TRelatedEntity>(string navigationName)
                => new NonGenericTestOwnedNavigationBuilder<TEntity, TRelatedEntity>(
                    EntityTypeBuilder.OwnsMany(typeof(TRelatedEntity), navigationName));
            public override TestEntityTypeBuilder<TEntity> OwnsMany<TRelatedEntity>(
                string navigationName,
                Action<TestOwnedNavigationBuilder<TEntity, TRelatedEntity>> buildAction)
                => Wrap(
                    EntityTypeBuilder.OwnsMany(
                        typeof(TRelatedEntity),
                        navigationName,
                        r => buildAction(new NonGenericTestOwnedNavigationBuilder<TEntity, TRelatedEntity>(r))));

            public override TestOwnedNavigationBuilder<TEntity, TRelatedEntity> OwnsMany<TRelatedEntity>(
                Expression<Func<TEntity, IEnumerable<TRelatedEntity>>> navigationExpression)
                => new NonGenericTestOwnedNavigationBuilder<TEntity, TRelatedEntity>(
                    EntityTypeBuilder.OwnsMany(typeof(TRelatedEntity), navigationExpression.GetMemberAccess().GetSimpleMemberName()));

            public override TestEntityTypeBuilder<TEntity> OwnsMany<TRelatedEntity>(
                Expression<Func<TEntity, IEnumerable<TRelatedEntity>>> navigationExpression,
                Action<TestOwnedNavigationBuilder<TEntity, TRelatedEntity>> buildAction)
                => Wrap(
                    EntityTypeBuilder.OwnsMany(
                        typeof(TRelatedEntity),
                        navigationExpression.GetMemberAccess().GetSimpleMemberName(),
                        r => buildAction(new NonGenericTestOwnedNavigationBuilder<TEntity, TRelatedEntity>(r))));

            public override TestReferenceNavigationBuilder<TEntity, TRelatedEntity> HasOne<TRelatedEntity>(string navigationName)
                => new NonGenericTestReferenceNavigationBuilder<TEntity, TRelatedEntity>(
                    EntityTypeBuilder.HasOne(navigationName));

            public override TestReferenceNavigationBuilder<TEntity, TRelatedEntity> HasOne<TRelatedEntity>(
                Expression<Func<TEntity, TRelatedEntity>> navigationExpression = null)
                => new NonGenericTestReferenceNavigationBuilder<TEntity, TRelatedEntity>(
                    EntityTypeBuilder.HasOne(
                        typeof(TRelatedEntity),
                        navigationExpression?.GetMemberAccess().GetSimpleMemberName()));

            public override TestCollectionNavigationBuilder<TEntity, TRelatedEntity> HasMany<TRelatedEntity>(string navigationName)
                => new NonGenericTestCollectionNavigationBuilder<TEntity, TRelatedEntity>(
                    EntityTypeBuilder.HasMany(typeof(TRelatedEntity), navigationName));

            public override TestCollectionNavigationBuilder<TEntity, TRelatedEntity> HasMany<TRelatedEntity>(
                Expression<Func<TEntity, IEnumerable<TRelatedEntity>>> navigationExpression = null)
                => new NonGenericTestCollectionNavigationBuilder<TEntity, TRelatedEntity>(
                    EntityTypeBuilder.HasMany(
                        typeof(TRelatedEntity),
                        navigationExpression?.GetMemberAccess().GetSimpleMemberName()));

            public override TestEntityTypeBuilder<TEntity> HasQueryFilter(Expression<Func<TEntity, bool>> filter)
                => Wrap(EntityTypeBuilder.HasQueryFilter(filter));

            public override TestEntityTypeBuilder<TEntity> HasChangeTrackingStrategy(ChangeTrackingStrategy changeTrackingStrategy)
                => Wrap(EntityTypeBuilder.HasChangeTrackingStrategy(changeTrackingStrategy));

            public override TestEntityTypeBuilder<TEntity> UsePropertyAccessMode(PropertyAccessMode propertyAccessMode)
                => Wrap(EntityTypeBuilder.UsePropertyAccessMode(propertyAccessMode));

            public override DataBuilder<TEntity> HasData(params TEntity[] data)
            {
                EntityTypeBuilder.HasData(data);
                return new DataBuilder<TEntity>();
            }

            public override DataBuilder<TEntity> HasData(params object[] data)
            {
                EntityTypeBuilder.HasData(data);
                return new DataBuilder<TEntity>();
            }

            public override DataBuilder<TEntity> HasData(IEnumerable<TEntity> data)
            {
                EntityTypeBuilder.HasData(data);
                return new DataBuilder<TEntity>();
            }

            public override DataBuilder<TEntity> HasData(IEnumerable<object> data)
            {
                EntityTypeBuilder.HasData(data);
                return new DataBuilder<TEntity>();
            }

            public override TestDiscriminatorBuilder<TDiscriminator> HasDiscriminator<TDiscriminator>(
                Expression<Func<TEntity, TDiscriminator>> propertyExpression)
                => new NonGenericTestDiscriminatorBuilder<TDiscriminator>(
                    EntityTypeBuilder.HasDiscriminator(
                        propertyExpression?.GetMemberAccess().GetSimpleMemberName(), typeof(TDiscriminator)));

            public override TestDiscriminatorBuilder<TDiscriminator> HasDiscriminator<TDiscriminator>(string propertyName)
                => new NonGenericTestDiscriminatorBuilder<TDiscriminator>(
                    EntityTypeBuilder.HasDiscriminator(propertyName, typeof(TDiscriminator)));

            public override TestEntityTypeBuilder<TEntity> HasNoDiscriminator()
                => Wrap(EntityTypeBuilder.HasNoDiscriminator());

            public EntityTypeBuilder Instance => EntityTypeBuilder;
        }

        protected class NonGenericTestDiscriminatorBuilder<TDiscriminator> : TestDiscriminatorBuilder<TDiscriminator>
        {
            public NonGenericTestDiscriminatorBuilder(DiscriminatorBuilder discriminatorBuilder)
            {
                DiscriminatorBuilder = discriminatorBuilder;
            }

            protected DiscriminatorBuilder DiscriminatorBuilder { get; }

            protected virtual TestDiscriminatorBuilder<TDiscriminator> Wrap(DiscriminatorBuilder discriminatorBuilder)
                => new NonGenericTestDiscriminatorBuilder<TDiscriminator>(discriminatorBuilder);

            public override TestDiscriminatorBuilder<TDiscriminator> IsComplete(bool complete)
                => Wrap(DiscriminatorBuilder.IsComplete(complete));

            public override TestDiscriminatorBuilder<TDiscriminator> HasValue(TDiscriminator value)
                => Wrap(DiscriminatorBuilder.HasValue(value));

            public override TestDiscriminatorBuilder<TDiscriminator> HasValue<TEntity>(TDiscriminator value)
                => Wrap(DiscriminatorBuilder.HasValue<TEntity>(value));

            public override TestDiscriminatorBuilder<TDiscriminator> HasValue(Type entityType, TDiscriminator value)
                => Wrap(DiscriminatorBuilder.HasValue(entityType, value));

            public override TestDiscriminatorBuilder<TDiscriminator> HasValue(string entityTypeName, TDiscriminator value)
                => Wrap(DiscriminatorBuilder.HasValue(entityTypeName, value));
        }

        protected class NonGenericTestOwnedEntityTypeBuilder<TEntity> : TestOwnedEntityTypeBuilder<TEntity>,
            IInfrastructure<OwnedEntityTypeBuilder>
            where TEntity : class
        {
            public NonGenericTestOwnedEntityTypeBuilder(OwnedEntityTypeBuilder ownedEntityTypeBuilder)
            {
                OwnedEntityTypeBuilder = ownedEntityTypeBuilder;
            }

            protected OwnedEntityTypeBuilder OwnedEntityTypeBuilder { get; }

            public OwnedEntityTypeBuilder Instance => OwnedEntityTypeBuilder;
        }

        protected class NonGenericTestSharedEntityTypeBuilder<TEntity> : TestSharedEntityTypeBuilder<TEntity>,
            IInfrastructure<SharedEntityTypeBuilder>
            where TEntity : class
        {
            public NonGenericTestSharedEntityTypeBuilder(SharedEntityTypeBuilder sharedEntityTypeBuilder)
            {
                SharedEntityTypeBuilder = sharedEntityTypeBuilder;
            }

            protected SharedEntityTypeBuilder SharedEntityTypeBuilder { get; }

            public SharedEntityTypeBuilder Instance => SharedEntityTypeBuilder;
        }

        protected class NonGenericTestPropertyBuilder<TProperty> : TestPropertyBuilder<TProperty>, IInfrastructure<PropertyBuilder>
        {
            public NonGenericTestPropertyBuilder(PropertyBuilder propertyBuilder)
            {
                PropertyBuilder = propertyBuilder;
            }

            private PropertyBuilder PropertyBuilder { get; }

            public override IMutableProperty Metadata => PropertyBuilder.Metadata;

            public override TestPropertyBuilder<TProperty> HasAnnotation(string annotation, object value)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasAnnotation(annotation, value));

            public override TestPropertyBuilder<TProperty> IsRequired(bool isRequired = true)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.IsRequired(isRequired));

            public override TestPropertyBuilder<TProperty> HasMaxLength(int maxLength)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasMaxLength(maxLength));

            public override TestPropertyBuilder<TProperty> IsUnicode(bool unicode = true)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.IsUnicode(unicode));

            public override TestPropertyBuilder<TProperty> IsRowVersion()
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.IsRowVersion());

            public override TestPropertyBuilder<TProperty> IsConcurrencyToken(bool isConcurrencyToken = true)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.IsConcurrencyToken(isConcurrencyToken));

            public override TestPropertyBuilder<TProperty> ValueGeneratedNever()
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.ValueGeneratedNever());

            public override TestPropertyBuilder<TProperty> ValueGeneratedOnAdd()
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.ValueGeneratedOnAdd());

            public override TestPropertyBuilder<TProperty> ValueGeneratedOnAddOrUpdate()
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.ValueGeneratedOnAddOrUpdate());

            public override TestPropertyBuilder<TProperty> ValueGeneratedOnUpdate()
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.ValueGeneratedOnUpdate());

            public override TestPropertyBuilder<TProperty> HasValueGenerator<TGenerator>()
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasValueGenerator<TGenerator>());

            public override TestPropertyBuilder<TProperty> HasValueGenerator(Type valueGeneratorType)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasValueGenerator(valueGeneratorType));

            public override TestPropertyBuilder<TProperty> HasValueGenerator(Func<IProperty, IEntityType, ValueGenerator> factory)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasValueGenerator(factory));

            public override TestPropertyBuilder<TProperty> HasField(string fieldName)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasField(fieldName));

            public override TestPropertyBuilder<TProperty> UsePropertyAccessMode(PropertyAccessMode propertyAccessMode)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.UsePropertyAccessMode(propertyAccessMode));

            public override TestPropertyBuilder<TProperty> HasConversion<TProvider>()
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasConversion<TProvider>());

            public override TestPropertyBuilder<TProperty> HasConversion(Type providerClrType)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasConversion(providerClrType));

            public override TestPropertyBuilder<TProperty> HasConversion<TProvider>(
                Expression<Func<TProperty, TProvider>> convertToProviderExpression,
                Expression<Func<TProvider, TProperty>> convertFromProviderExpression)
                => new NonGenericTestPropertyBuilder<TProperty>(
                    PropertyBuilder.HasConversion(
                        new ValueConverter<TProperty, TProvider>(convertToProviderExpression, convertFromProviderExpression)));

            public override TestPropertyBuilder<TProperty> HasConversion<TStore>(ValueConverter<TProperty, TStore> converter)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasConversion(converter));

            public override TestPropertyBuilder<TProperty> HasConversion(ValueConverter converter)
                => new NonGenericTestPropertyBuilder<TProperty>(PropertyBuilder.HasConversion(converter));

            PropertyBuilder IInfrastructure<PropertyBuilder>.Instance => PropertyBuilder;
        }

        protected class NonGenericTestNavigationBuilder : TestNavigationBuilder
        {
            public NonGenericTestNavigationBuilder(NavigationBuilder navigationBuilder)
            {
                NavigationBuilder = navigationBuilder;
            }

            private NavigationBuilder NavigationBuilder { get; }

            public override TestNavigationBuilder HasAnnotation(string annotation, object value)
                => new NonGenericTestNavigationBuilder(NavigationBuilder.HasAnnotation(annotation, value));

            public override TestNavigationBuilder UsePropertyAccessMode(PropertyAccessMode propertyAccessMode)
                => new NonGenericTestNavigationBuilder(NavigationBuilder.UsePropertyAccessMode(propertyAccessMode));
        }

        protected class NonGenericTestKeyBuilder<TEntity> : TestKeyBuilder<TEntity>, IInfrastructure<KeyBuilder>
        {
            public NonGenericTestKeyBuilder(KeyBuilder keyBuilder)
            {
                KeyBuilder = keyBuilder;
            }

            private KeyBuilder KeyBuilder { get; }

            public override IMutableKey Metadata => KeyBuilder.Metadata;

            public override TestKeyBuilder<TEntity> HasAnnotation(string annotation, object value)
                => new NonGenericTestKeyBuilder<TEntity>(KeyBuilder.HasAnnotation(annotation, value));

            KeyBuilder IInfrastructure<KeyBuilder>.Instance => KeyBuilder;
        }

        public class NonGenericTestIndexBuilder<TEntity> : TestIndexBuilder<TEntity>, IInfrastructure<IndexBuilder>
        {
            public NonGenericTestIndexBuilder(IndexBuilder indexBuilder)
            {
                IndexBuilder = indexBuilder;
            }

            private IndexBuilder IndexBuilder { get; }
            public override IMutableIndex Metadata => IndexBuilder.Metadata;

            public override TestIndexBuilder<TEntity> HasAnnotation(string annotation, object value)
                => new NonGenericTestIndexBuilder<TEntity>(IndexBuilder.HasAnnotation(annotation, value));

            public override TestIndexBuilder<TEntity> IsUnique(bool isUnique = true)
                => new NonGenericTestIndexBuilder<TEntity>(IndexBuilder.IsUnique(isUnique));

            IndexBuilder IInfrastructure<IndexBuilder>.Instance => IndexBuilder;
        }

        protected class
            NonGenericTestReferenceNavigationBuilder<TEntity, TRelatedEntity> : TestReferenceNavigationBuilder<TEntity, TRelatedEntity>
            where TEntity : class
            where TRelatedEntity : class
        {
            public NonGenericTestReferenceNavigationBuilder(ReferenceNavigationBuilder referenceNavigationBuilder)
            {
                ReferenceNavigationBuilder = referenceNavigationBuilder;
            }

            protected ReferenceNavigationBuilder ReferenceNavigationBuilder { get; }

            public override TestReferenceCollectionBuilder<TRelatedEntity, TEntity> WithMany(string navigationName)
                => new NonGenericTestReferenceCollectionBuilder<TRelatedEntity, TEntity>(
                    ReferenceNavigationBuilder.WithMany(navigationName));

            public override TestReferenceCollectionBuilder<TRelatedEntity, TEntity> WithMany(
                Expression<Func<TRelatedEntity, IEnumerable<TEntity>>> navigationExpression = null)
                => new NonGenericTestReferenceCollectionBuilder<TRelatedEntity, TEntity>(
                    ReferenceNavigationBuilder.WithMany(
                        navigationExpression?.GetMemberAccess().GetSimpleMemberName()));

            public override TestReferenceReferenceBuilder<TEntity, TRelatedEntity> WithOne(string navigationName)
                => new NonGenericTestReferenceReferenceBuilder<TEntity, TRelatedEntity>(
                    ReferenceNavigationBuilder.WithOne(navigationName));

            public override TestReferenceReferenceBuilder<TEntity, TRelatedEntity> WithOne(
                Expression<Func<TRelatedEntity, TEntity>> navigationExpression = null)
                => new NonGenericTestReferenceReferenceBuilder<TEntity, TRelatedEntity>(
                    ReferenceNavigationBuilder.WithOne(
                        navigationExpression?.GetMemberAccess().GetSimpleMemberName()));
        }

        protected class NonGenericTestCollectionNavigationBuilder<TEntity, TRelatedEntity>
            : TestCollectionNavigationBuilder<TEntity, TRelatedEntity>
            where TEntity : class
            where TRelatedEntity : class
        {
            public NonGenericTestCollectionNavigationBuilder(CollectionNavigationBuilder collectionNavigationBuilder)
            {
                CollectionNavigationBuilder = collectionNavigationBuilder;
            }

            private CollectionNavigationBuilder CollectionNavigationBuilder { get; }

            public override TestReferenceCollectionBuilder<TEntity, TRelatedEntity> WithOne(
                string navigationName)
                => new NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>(
                    CollectionNavigationBuilder.WithOne(navigationName));

            public override TestReferenceCollectionBuilder<TEntity, TRelatedEntity> WithOne(
                Expression<Func<TRelatedEntity, TEntity>> navigationExpression = null)
                => new NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>(
                    CollectionNavigationBuilder.WithOne(
                        navigationExpression?.GetMemberAccess().GetSimpleMemberName()));

            public override TestCollectionCollectionBuilder<TRelatedEntity, TEntity> WithMany(
                string navigationName)
                => new NonGenericTestCollectionCollectionBuilder<TRelatedEntity, TEntity>(
                    CollectionNavigationBuilder.WithMany(navigationName));

            public override TestCollectionCollectionBuilder<TRelatedEntity, TEntity> WithMany(
                Expression<Func<TRelatedEntity, IEnumerable<TEntity>>> navigationExpression)
                => new NonGenericTestCollectionCollectionBuilder<TRelatedEntity, TEntity>(
                    CollectionNavigationBuilder.WithMany(navigationExpression.GetMemberAccess().GetSimpleMemberName()));
        }

        protected class NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>
            : TestReferenceCollectionBuilder<TEntity, TRelatedEntity>
            where TEntity : class
            where TRelatedEntity : class
        {
            public NonGenericTestReferenceCollectionBuilder(ReferenceCollectionBuilder referenceCollectionBuilder)
            {
                ReferenceCollectionBuilder = referenceCollectionBuilder;
            }

            public ReferenceCollectionBuilder ReferenceCollectionBuilder { get; }

            public override IMutableForeignKey Metadata => ReferenceCollectionBuilder.Metadata;

            public override TestReferenceCollectionBuilder<TEntity, TRelatedEntity> HasForeignKey(
                Expression<Func<TRelatedEntity, object>> foreignKeyExpression)
                => new NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>(
                    ReferenceCollectionBuilder.HasForeignKey(
                        foreignKeyExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestReferenceCollectionBuilder<TEntity, TRelatedEntity> HasPrincipalKey(
                Expression<Func<TEntity, object>> keyExpression)
                => new NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>(
                    ReferenceCollectionBuilder.HasPrincipalKey(
                        keyExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestReferenceCollectionBuilder<TEntity, TRelatedEntity> HasForeignKey(params string[] foreignKeyPropertyNames)
                => new NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>(
                    ReferenceCollectionBuilder.HasForeignKey(foreignKeyPropertyNames));

            public override TestReferenceCollectionBuilder<TEntity, TRelatedEntity> HasPrincipalKey(params string[] keyPropertyNames)
                => new NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>(
                    ReferenceCollectionBuilder.HasPrincipalKey(keyPropertyNames));

            public override TestReferenceCollectionBuilder<TEntity, TRelatedEntity> HasAnnotation(string annotation, object value)
                => new NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>(
                    ReferenceCollectionBuilder.HasAnnotation(annotation, value));

            public override TestReferenceCollectionBuilder<TEntity, TRelatedEntity> IsRequired(bool isRequired = true)
                => new NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>(ReferenceCollectionBuilder.IsRequired(isRequired));

            public override TestReferenceCollectionBuilder<TEntity, TRelatedEntity> OnDelete(DeleteBehavior deleteBehavior)
                => new NonGenericTestReferenceCollectionBuilder<TEntity, TRelatedEntity>(
                    ReferenceCollectionBuilder.OnDelete(deleteBehavior));
        }

        protected class
            NonGenericTestReferenceReferenceBuilder<TEntity, TRelatedEntity> : TestReferenceReferenceBuilder<TEntity, TRelatedEntity>
            where TEntity : class
            where TRelatedEntity : class
        {
            public NonGenericTestReferenceReferenceBuilder(ReferenceReferenceBuilder referenceReferenceBuilder)
            {
                ReferenceReferenceBuilder = referenceReferenceBuilder;
            }

            protected ReferenceReferenceBuilder ReferenceReferenceBuilder { get; }

            public override IMutableForeignKey Metadata => ReferenceReferenceBuilder.Metadata;

            protected virtual NonGenericTestReferenceReferenceBuilder<TEntity, TRelatedEntity> Wrap(
                ReferenceReferenceBuilder referenceReferenceBuilder)
                => new NonGenericTestReferenceReferenceBuilder<TEntity, TRelatedEntity>(referenceReferenceBuilder);

            public override TestReferenceReferenceBuilder<TEntity, TRelatedEntity> HasAnnotation(string annotation, object value)
                => Wrap(ReferenceReferenceBuilder.HasAnnotation(annotation, value));

            public override TestReferenceReferenceBuilder<TEntity, TRelatedEntity> HasForeignKey<TDependentEntity>(
                Expression<Func<TDependentEntity, object>> foreignKeyExpression)
                => Wrap(ReferenceReferenceBuilder.HasForeignKey(
                    typeof(TDependentEntity),
                    foreignKeyExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestReferenceReferenceBuilder<TEntity, TRelatedEntity> HasPrincipalKey<TPrincipalEntity>(
                Expression<Func<TPrincipalEntity, object>> keyExpression)
                => Wrap(ReferenceReferenceBuilder.HasPrincipalKey(
                    typeof(TPrincipalEntity), keyExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestReferenceReferenceBuilder<TEntity, TRelatedEntity> HasForeignKey<TDependentEntity>(
                params string[] foreignKeyPropertyNames)
                => Wrap(ReferenceReferenceBuilder.HasForeignKey(typeof(TDependentEntity), foreignKeyPropertyNames));

            public override TestReferenceReferenceBuilder<TEntity, TRelatedEntity> HasPrincipalKey<TPrincipalEntity>(
                params string[] keyPropertyNames)
                => Wrap(ReferenceReferenceBuilder.HasPrincipalKey(typeof(TPrincipalEntity), keyPropertyNames));

            public override TestReferenceReferenceBuilder<TEntity, TRelatedEntity> IsRequired(bool isRequired = true)
                => Wrap(ReferenceReferenceBuilder.IsRequired(isRequired));

            public override TestReferenceReferenceBuilder<TEntity, TRelatedEntity> OnDelete(DeleteBehavior deleteBehavior)
                => Wrap(ReferenceReferenceBuilder.OnDelete(deleteBehavior));
        }

        protected class NonGenericTestCollectionCollectionBuilder<TLeftEntity, TRightEntity> :
            TestCollectionCollectionBuilder<TLeftEntity, TRightEntity>
            where TLeftEntity : class
            where TRightEntity : class
        {
            public NonGenericTestCollectionCollectionBuilder(CollectionCollectionBuilder collectionCollectionBuilder)
            {
                CollectionCollectionBuilder = collectionCollectionBuilder;
            }

            protected CollectionCollectionBuilder CollectionCollectionBuilder { get; }

            public override TestEntityTypeBuilder<TAssociationEntity> UsingEntity<TAssociationEntity>(
                Func<TestEntityTypeBuilder<TAssociationEntity>,
                    TestReferenceCollectionBuilder<TLeftEntity, TAssociationEntity>> configureRight,
                Func<TestEntityTypeBuilder<TAssociationEntity>,
                    TestReferenceCollectionBuilder<TRightEntity, TAssociationEntity>> configureLeft)
                => new NonGenericTestEntityTypeBuilder<TAssociationEntity>(CollectionCollectionBuilder.UsingEntity(
                    typeof(TAssociationEntity),
                    l => ((NonGenericTestReferenceCollectionBuilder<TLeftEntity, TAssociationEntity>)configureRight(
                        new NonGenericTestEntityTypeBuilder<TAssociationEntity>(l))).ReferenceCollectionBuilder,
                    r => ((NonGenericTestReferenceCollectionBuilder<TRightEntity, TAssociationEntity>)configureLeft(
                        new NonGenericTestEntityTypeBuilder<TAssociationEntity>(r))).ReferenceCollectionBuilder));

            public override TestEntityTypeBuilder<TAssociationEntity> UsingEntity<TAssociationEntity>(
                string joinEntityName,
                Func<TestEntityTypeBuilder<TAssociationEntity>,
                    TestReferenceCollectionBuilder<TLeftEntity, TAssociationEntity>> configureRight,
                Func<TestEntityTypeBuilder<TAssociationEntity>,
                    TestReferenceCollectionBuilder<TRightEntity, TAssociationEntity>> configureLeft)
                => new NonGenericTestEntityTypeBuilder<TAssociationEntity>(CollectionCollectionBuilder.UsingEntity(
                    joinEntityName,
                    typeof(TAssociationEntity),
                    l => ((NonGenericTestReferenceCollectionBuilder<TLeftEntity, TAssociationEntity>)configureRight(
                        new NonGenericTestEntityTypeBuilder<TAssociationEntity>(l))).ReferenceCollectionBuilder,
                    r => ((NonGenericTestReferenceCollectionBuilder<TRightEntity, TAssociationEntity>)configureLeft(
                        new NonGenericTestEntityTypeBuilder<TAssociationEntity>(r))).ReferenceCollectionBuilder));

            public override TestEntityTypeBuilder<TRightEntity> UsingEntity<TAssociationEntity>(
                Func<TestEntityTypeBuilder<TAssociationEntity>,
                    TestReferenceCollectionBuilder<TLeftEntity, TAssociationEntity>> configureRight,
                Func<TestEntityTypeBuilder<TAssociationEntity>,
                    TestReferenceCollectionBuilder<TRightEntity, TAssociationEntity>> configureLeft,
                Action<TestEntityTypeBuilder<TAssociationEntity>> configureAssociation)
                where TAssociationEntity : class
                => new NonGenericTestEntityTypeBuilder<TRightEntity>(CollectionCollectionBuilder.UsingEntity(
                    typeof(TAssociationEntity),
                    l => ((NonGenericTestReferenceCollectionBuilder<TLeftEntity, TAssociationEntity>)configureRight(
                        new NonGenericTestEntityTypeBuilder<TAssociationEntity>(l))).ReferenceCollectionBuilder,
                    r => ((NonGenericTestReferenceCollectionBuilder<TRightEntity, TAssociationEntity>)configureLeft(
                        new NonGenericTestEntityTypeBuilder<TAssociationEntity>(r))).ReferenceCollectionBuilder,
                    e => configureAssociation(new NonGenericTestEntityTypeBuilder<TAssociationEntity>(e))));

            public override TestEntityTypeBuilder<TRightEntity> UsingEntity<TAssociationEntity>(
                string joinEntityName,
                Func<TestEntityTypeBuilder<TAssociationEntity>,
                    TestReferenceCollectionBuilder<TLeftEntity, TAssociationEntity>> configureRight,
                Func<TestEntityTypeBuilder<TAssociationEntity>,
                    TestReferenceCollectionBuilder<TRightEntity, TAssociationEntity>> configureLeft,
                Action<TestEntityTypeBuilder<TAssociationEntity>> configureAssociation)
                where TAssociationEntity : class
                => new NonGenericTestEntityTypeBuilder<TRightEntity>(CollectionCollectionBuilder.UsingEntity(
                    joinEntityName,
                    typeof(TAssociationEntity),
                    l => ((NonGenericTestReferenceCollectionBuilder<TLeftEntity, TAssociationEntity>)configureRight(
                        new NonGenericTestEntityTypeBuilder<TAssociationEntity>(l))).ReferenceCollectionBuilder,
                    r => ((NonGenericTestReferenceCollectionBuilder<TRightEntity, TAssociationEntity>)configureLeft(
                        new NonGenericTestEntityTypeBuilder<TAssociationEntity>(r))).ReferenceCollectionBuilder,
                    e => configureAssociation(new NonGenericTestEntityTypeBuilder<TAssociationEntity>(e))));
        }

        protected class NonGenericTestOwnershipBuilder<TEntity, TRelatedEntity>
            : TestOwnershipBuilder<TEntity, TRelatedEntity>, IInfrastructure<OwnershipBuilder>
            where TEntity : class
            where TRelatedEntity : class
        {
            public NonGenericTestOwnershipBuilder(OwnershipBuilder ownershipBuilder)
            {
                OwnershipBuilder = ownershipBuilder;
            }

            protected OwnershipBuilder OwnershipBuilder { get; }

            public override IMutableForeignKey Metadata => OwnershipBuilder.Metadata;

            protected virtual NonGenericTestOwnershipBuilder<TNewEntity, TNewRelatedEntity> Wrap<TNewEntity, TNewRelatedEntity>(
                OwnershipBuilder ownershipBuilder)
                where TNewEntity : class
                where TNewRelatedEntity : class
                => new NonGenericTestOwnershipBuilder<TNewEntity, TNewRelatedEntity>(ownershipBuilder);

            public override TestOwnershipBuilder<TEntity, TRelatedEntity> HasAnnotation(string annotation, object value)
                => Wrap<TEntity, TRelatedEntity>(OwnershipBuilder.HasAnnotation(annotation, value));

            public override TestOwnershipBuilder<TEntity, TRelatedEntity> HasForeignKey(
                params string[] foreignKeyPropertyNames)
                => Wrap<TEntity, TRelatedEntity>(OwnershipBuilder.HasForeignKey(foreignKeyPropertyNames));

            public override TestOwnershipBuilder<TEntity, TRelatedEntity> HasForeignKey(
                Expression<Func<TRelatedEntity, object>> foreignKeyExpression)
                => Wrap<TEntity, TRelatedEntity>(
                    OwnershipBuilder.HasForeignKey(
                        foreignKeyExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestOwnershipBuilder<TEntity, TRelatedEntity> HasPrincipalKey(
                params string[] keyPropertyNames)
                => Wrap<TEntity, TRelatedEntity>(OwnershipBuilder.HasPrincipalKey(keyPropertyNames));

            public override TestOwnershipBuilder<TEntity, TRelatedEntity> HasPrincipalKey(
                Expression<Func<TEntity, object>> keyExpression)
                => Wrap<TEntity, TRelatedEntity>(
                    OwnershipBuilder.HasPrincipalKey(
                        keyExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            OwnershipBuilder IInfrastructure<OwnershipBuilder>.Instance => OwnershipBuilder;
        }

        protected class NonGenericTestOwnedNavigationBuilder<TEntity, TDependentEntity>
            : TestOwnedNavigationBuilder<TEntity, TDependentEntity>, IInfrastructure<OwnedNavigationBuilder>
            where TEntity : class
            where TDependentEntity : class
        {
            public NonGenericTestOwnedNavigationBuilder(OwnedNavigationBuilder ownedNavigationBuilder)
            {
                OwnedNavigationBuilder = ownedNavigationBuilder;
            }

            protected OwnedNavigationBuilder OwnedNavigationBuilder { get; }

            public override IMutableForeignKey Metadata => OwnedNavigationBuilder.Metadata;
            public override IMutableEntityType OwnedEntityType => OwnedNavigationBuilder.OwnedEntityType;

            protected virtual NonGenericTestOwnedNavigationBuilder<TNewEntity, TNewDependentEntity> Wrap<TNewEntity, TNewDependentEntity>(
                OwnedNavigationBuilder ownedNavigationBuilder)
                where TNewEntity : class
                where TNewDependentEntity : class
                => new NonGenericTestOwnedNavigationBuilder<TNewEntity, TNewDependentEntity>(ownedNavigationBuilder);

            public override TestOwnedNavigationBuilder<TEntity, TDependentEntity> HasAnnotation(
                string annotation, object value)
                => Wrap<TEntity, TDependentEntity>(OwnedNavigationBuilder.HasAnnotation(annotation, value));

            public override TestKeyBuilder<TDependentEntity> HasKey(Expression<Func<TDependentEntity, object>> keyExpression)
                => new NonGenericTestKeyBuilder<TDependentEntity>(
                    OwnedNavigationBuilder.HasKey(
                        keyExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestKeyBuilder<TDependentEntity> HasKey(params string[] propertyNames)
                => new NonGenericTestKeyBuilder<TDependentEntity>(OwnedNavigationBuilder.HasKey(propertyNames));

            public override TestPropertyBuilder<TProperty> Property<TProperty>(string propertyName)
                => new NonGenericTestPropertyBuilder<TProperty>(OwnedNavigationBuilder.Property<TProperty>(propertyName));

            public override TestPropertyBuilder<TProperty> IndexerProperty<TProperty>(string propertyName)
                => new NonGenericTestPropertyBuilder<TProperty>(OwnedNavigationBuilder.IndexerProperty<TProperty>(propertyName));

            public override TestPropertyBuilder<TProperty> Property<TProperty>(
                Expression<Func<TDependentEntity, TProperty>> propertyExpression)
            {
                var memberInfo = propertyExpression.GetMemberAccess();
                return new NonGenericTestPropertyBuilder<TProperty>(
                    OwnedNavigationBuilder.Property(memberInfo.GetMemberType(), memberInfo.GetSimpleMemberName()));
            }

            public override TestNavigationBuilder Navigation<TNavigation>(string navigationName)
                => new NonGenericTestNavigationBuilder(OwnedNavigationBuilder.Navigation(navigationName));
            public override TestNavigationBuilder Navigation<TNavigation>(
                Expression<Func<TDependentEntity, TNavigation>> navigationExpression)
                => new NonGenericTestNavigationBuilder(
                    OwnedNavigationBuilder.Navigation(navigationExpression.GetMemberAccess().GetSimpleMemberName()));

            public override TestOwnedNavigationBuilder<TEntity, TDependentEntity> Ignore(string propertyName)
                => Wrap<TEntity, TDependentEntity>(OwnedNavigationBuilder.Ignore(propertyName));

            public override TestOwnedNavigationBuilder<TEntity, TDependentEntity> Ignore(
                Expression<Func<TDependentEntity, object>> propertyExpression)
                => Wrap<TEntity, TDependentEntity>(
                    OwnedNavigationBuilder.Ignore(propertyExpression.GetMemberAccess().GetSimpleMemberName()));

            public override TestIndexBuilder<TEntity> HasIndex(params string[] propertyNames)
                => new NonGenericTestIndexBuilder<TEntity>(OwnedNavigationBuilder.HasIndex(propertyNames));

            public override TestIndexBuilder<TEntity> HasIndex(Expression<Func<TDependentEntity, object>> indexExpression)
                => new NonGenericTestIndexBuilder<TEntity>(
                    OwnedNavigationBuilder.HasIndex(
                        indexExpression.GetMemberAccessList().Select(p => p.GetSimpleMemberName()).ToArray()));

            public override TestOwnershipBuilder<TEntity, TDependentEntity> WithOwner(string ownerReference)
                => new NonGenericTestOwnershipBuilder<TEntity, TDependentEntity>(
                    OwnedNavigationBuilder.WithOwner(ownerReference));

            public override TestOwnershipBuilder<TEntity, TDependentEntity> WithOwner(
                Expression<Func<TDependentEntity, TEntity>> referenceExpression)
                => new NonGenericTestOwnershipBuilder<TEntity, TDependentEntity>(
                    OwnedNavigationBuilder.WithOwner(referenceExpression?.GetMemberAccess()?.GetSimpleMemberName()));

            public override TestOwnedNavigationBuilder<TDependentEntity, TNewDependentEntity> OwnsOne<TNewDependentEntity>(
                Expression<Func<TDependentEntity, TNewDependentEntity>> navigationExpression)
                => Wrap<TDependentEntity, TNewDependentEntity>(
                    OwnedNavigationBuilder.OwnsOne(
                        typeof(TNewDependentEntity), navigationExpression.GetMemberAccess().GetSimpleMemberName()));

            public override TestOwnedNavigationBuilder<TEntity, TDependentEntity> OwnsOne<TNewDependentEntity>(
                Expression<Func<TDependentEntity, TNewDependentEntity>> navigationExpression,
                Action<TestOwnedNavigationBuilder<TDependentEntity, TNewDependentEntity>> buildAction)
                => Wrap<TEntity, TDependentEntity>(
                    OwnedNavigationBuilder.OwnsOne(
                        typeof(TNewDependentEntity),
                        navigationExpression.GetMemberAccess().GetSimpleMemberName(),
                        r => buildAction(new NonGenericTestOwnedNavigationBuilder<TDependentEntity, TNewDependentEntity>(r))));

            public override TestOwnedNavigationBuilder<TDependentEntity, TNewDependentEntity> OwnsMany<TNewDependentEntity>(
                Expression<Func<TDependentEntity, IEnumerable<TNewDependentEntity>>> navigationExpression)
                => Wrap<TDependentEntity, TNewDependentEntity>(
                    OwnedNavigationBuilder.OwnsMany(
                        typeof(TNewDependentEntity), navigationExpression.GetMemberAccess().GetSimpleMemberName()));

            public override TestOwnedNavigationBuilder<TEntity, TDependentEntity> OwnsMany<TNewDependentEntity>(
                Expression<Func<TDependentEntity, IEnumerable<TNewDependentEntity>>> navigationExpression,
                Action<TestOwnedNavigationBuilder<TDependentEntity, TNewDependentEntity>> buildAction)
                => Wrap<TEntity, TDependentEntity>(
                    OwnedNavigationBuilder.OwnsMany(
                        typeof(TNewDependentEntity),
                        navigationExpression.GetMemberAccess().GetSimpleMemberName(),
                        r => buildAction(Wrap<TDependentEntity, TNewDependentEntity>(r))));

            public override TestReferenceNavigationBuilder<TDependentEntity, TNewDependentEntity> HasOne<TNewDependentEntity>(
                Expression<Func<TDependentEntity, TNewDependentEntity>> navigationExpression = null)
                => new NonGenericTestReferenceNavigationBuilder<TDependentEntity, TNewDependentEntity>(
                    OwnedNavigationBuilder.HasOne(
                        typeof(TNewDependentEntity), navigationExpression?.GetMemberAccess().GetSimpleMemberName()));

            public override TestOwnedNavigationBuilder<TEntity, TDependentEntity> HasChangeTrackingStrategy(
                ChangeTrackingStrategy changeTrackingStrategy)
                => Wrap<TEntity, TDependentEntity>(OwnedNavigationBuilder.HasChangeTrackingStrategy(changeTrackingStrategy));

            public override TestOwnedNavigationBuilder<TEntity, TDependentEntity> UsePropertyAccessMode(
                PropertyAccessMode propertyAccessMode)
                => Wrap<TEntity, TDependentEntity>(OwnedNavigationBuilder.UsePropertyAccessMode(propertyAccessMode));

            public override DataBuilder<TDependentEntity> HasData(params TDependentEntity[] data)
            {
                OwnedNavigationBuilder.HasData(data);
                return new DataBuilder<TDependentEntity>();
            }

            public override DataBuilder<TDependentEntity> HasData(params object[] data)
            {
                OwnedNavigationBuilder.HasData(data);
                return new DataBuilder<TDependentEntity>();
            }

            public override DataBuilder<TDependentEntity> HasData(IEnumerable<TDependentEntity> data)
            {
                OwnedNavigationBuilder.HasData(data);
                return new DataBuilder<TDependentEntity>();
            }

            public override DataBuilder<TDependentEntity> HasData(IEnumerable<object> data)
            {
                OwnedNavigationBuilder.HasData(data);
                return new DataBuilder<TDependentEntity>();
            }

            OwnedNavigationBuilder IInfrastructure<OwnedNavigationBuilder>.Instance => OwnedNavigationBuilder;
        }
    }
}
